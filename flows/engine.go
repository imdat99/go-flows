package flows

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/text"
	"github.com/imdat99/go-flows/internal/worker"
)

// Options configure an engine. The zero value works.
type Options struct {
	// InstanceID identifies this process in leases; unique per process
	// (default: hostname plus a random suffix).
	InstanceID string
	// Queues the engine runs tasks from (default: DefaultQueue).
	Queues []string
	// LeaseTTL bounds how long a crashed process holds its tasks (default
	// 30s); leases are renewed every third of it.
	LeaseTTL time.Duration
	// MaxWorkflowTasks and MaxActivities bound concurrency (default 64).
	MaxWorkflowTasks, MaxActivities int
	// DisableEager stops running activities in the worker that scheduled
	// them (eager execution: one round trip per activity step instead of
	// a poll).
	DisableEager bool
	// Logger receives the engine's logs (default: discarded).
	Logger *slog.Logger
	// StickyFor keeps an execution's next task on this process while its
	// replayed state is cached (default 10s); CacheSize bounds that cache
	// (default 1000 executions, -1 disables it).
	StickyFor time.Duration
	CacheSize int
	// CacheBytes bounds the cache by the estimated memory of the cached
	// executions (default 256 MiB, -1: bounded by CacheSize only); beyond
	// either bound the least recently used are evicted. The estimate is a
	// fixed few KiB per execution and per live coroutine, plus the bytes
	// of every payload, compensation data and error in the history
	// replayed so far (encoded sizes, not the decoded values' heap size).
	CacheBytes int64
	// PollMax bounds the idle polling interval (default 1s, at most
	// LeaseTTL/3 so that a crashed engine's work resumes within about
	// LeaseTTL; Postgres notifications wake pollers earlier).
	PollMax time.Duration
	// EagerWindow is how long an execution waits for its eager activities
	// before letting signals and timers in (default 200ms).
	EagerWindow time.Duration
	// Hook, when set, runs in the transaction of every end or park.
	Hook RunHook
	// Observer, when set, receives activity attempts and ends.
	Observer Observer
	// Tracer, when set, wraps activity attempts and workflow tasks in spans
	// (OpenTelemetry: package otelflows).
	Tracer Tracer
	// StuckTimeout bounds how long workflow code may run without yielding
	// to the engine (default 10s). Code that exceeds it is handed back;
	// exceeding it again at the same point of its history parks the
	// execution (ErrStuck), whichever engine ran the first attempt (the
	// count is in the database). Each attempt leaves a goroutine running
	// until its process exits.
	StuckTimeout time.Duration
	// LockHold bounds how long, past its ttl, an execution keeps a lock
	// (Lock) while none of its code or activities runs but one of its
	// activities waits (in its queue, for a retry, or for another engine
	// after its process died) or its next task waits in the queue (events
	// delivered, not claimed yet) (default 1h; negative: not at all, the
	// lock lapses after its ttl). It is recorded with the lock when taken.
	LockHold time.Duration
	// ListenDSN, when set, is the connection string of the connection the
	// engine LISTENs on for work notifications, instead of one taken out of
	// the pool: for pools behind PgBouncer in transaction mode (which drops
	// notifications), point it at Postgres directly or at a session-mode
	// pool. It holds one connection while the engine runs. The pool's hooks
	// (BeforeConnect credentials, AfterConnect, tracers) do not apply to it;
	// without sslmode, pgx's default (prefer) may fall back to plaintext:
	// set sslmode=verify-full for a direct connection. Only its syntax is
	// checked by New: a connection that fails is logged ("flows: watch
	// failed") and retried, the engine polling meanwhile.
	ListenDSN string
}

// Engine runs the workflows and activities defined in this binary against
// a Postgres database. Several processes can run engines on the same
// database: tasks are leased, and every write is fenced.
type Engine struct {
	st     store.Store
	w      *worker.Worker
	obs    Observer
	tracer Tracer
	log    *slog.Logger // never nil
}

// New returns a stopped engine on pool. Run Migrate first.
func New(pool *pgxpool.Pool, opts Options) (*Engine, error) {
	st := pg.New(pool)
	if opts.ListenDSN != "" {
		cfg, err := pgx.ParseConfig(opts.ListenDSN)
		if err != nil {
			return nil, fmt.Errorf("%w: ListenDSN: %w", ErrInvalidArgument, err)
		}
		st.SetListenConfig(cfg)
	}
	return newEngine(st, opts)
}

// Migrate creates or upgrades the engine's tables (schema "flows"). It is
// safe to run from several processes at once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error { return pg.Migrate(ctx, pool) }

func newEngine(st store.Store, opts Options) (*Engine, error) {
	return newEngineWith(st, workerRegistry{}, opts)
}

func newEngineWith(st store.Store, acts worker.Activities, opts Options) (*Engine, error) {
	if opts.InstanceID == "" {
		host, _ := os.Hostname()
		var b [4]byte
		_, _ = rand.Read(b[:])
		opts.InstanceID = host + "-" + hex.EncodeToString(b[:])
	}
	for _, q := range opts.Queues {
		if err := text.CheckName(ErrInvalidArgument, "queue name", q); err != nil {
			return nil, err
		}
	}
	if len(opts.Queues) == 0 {
		opts.Queues = []string{DefaultQueue}
	}
	if h := opts.Hook; h != nil {
		switch s := st.(type) {
		case *pg.Store:
			s.SetHook(func(ctx context.Context, tx pgx.Tx, e store.EndInfo) error { return h.RunEnded(ctx, tx, runInfo(e)) })
		case interface { // the memory store (tests): no transaction
			SetHook(func(context.Context, store.EndInfo) error)
		}:
			s.SetHook(func(ctx context.Context, e store.EndInfo) error { return h.RunEnded(ctx, nil, runInfo(e)) })
		}
	}
	var obs worker.Observer
	if opts.Observer != nil {
		obs = observer{opts.Observer}
	}
	var tr worker.Tracer
	if opts.Tracer != nil {
		tr = tracer{opts.Tracer}
	}
	w, err := worker.New(st, workerRegistry{}, acts, worker.Options{
		Owner: opts.InstanceID, Queues: opts.Queues, LeaseTTL: opts.LeaseTTL,
		MaxWorkflowTasks: opts.MaxWorkflowTasks, MaxActivities: opts.MaxActivities,
		Eager: !opts.DisableEager, EagerWindow: opts.EagerWindow, StickyFor: opts.StickyFor,
		PollMax: opts.PollMax, CacheSize: opts.CacheSize, CacheBytes: opts.CacheBytes, Errors: errorCodec{}, Logger: opts.Logger,
		Observer: obs, Tracer: tr, StuckTimeout: opts.StuckTimeout, LockHold: opts.LockHold,
	})
	if err != nil {
		return nil, fmt.Errorf("flows: %w", err)
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Engine{st: st, w: w, obs: opts.Observer, tracer: opts.Tracer, log: log}, nil
}

// Start starts running tasks. Without Start, an engine still starts
// workflows and sends signals (for client-only processes).
func (e *Engine) Start(ctx context.Context) error { return e.w.Start(ctx) }

// Stop stops taking tasks and waits for the running ones until ctx is done;
// those still running then are canceled and resumed by another instance.
// No new claim is started after Stop begins, apart from one already being
// issued: a claim in flight completes (its tasks run as part of the drain)
// within a short grace period, else it is canceled. Stop returns ctx's
// error only if running tasks were canceled; an idle engine whose deadline
// passes returns nil.
//
// Once ctx is done, Stop returns within about a second more even if tasks
// are blocked on a database that stopped answering (a failover, a network
// partition without RST): those tasks are abandoned, their leases expire
// and another instance resumes their work. An abandoned task ends by
// itself within about LeaseTTL (its writes are bounded by it); until then
// it holds its pool connection, so closing the pool may wait that long.
func (e *Engine) Stop(ctx context.Context) error { return e.w.Stop(ctx) }

// Signal sends a signal called name with payload (JSON-encoded) to the
// running execution of instanceID; workflows receive it with Signals or
// Await. ErrNotFound / ErrNotActive when there is no running execution.
func (e *Engine) Signal(ctx context.Context, instanceID, name string, payload any) error {
	if err := errors.Join(text.CheckName(ErrInvalidArgument, "instance id", instanceID),
		text.CheckName(ErrInvalidArgument, "signal name", name)); err != nil {
		return err
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("flows: encode signal payload: %w", err)
	}
	return e.st.Signal(ctx, instanceID, name, b)
}

// Cancel asks the running execution of instanceID to cancel: its root
// context is canceled, which cancels its activities, timers and children.
// The workflow decides what to do (it can clean up in a Disconnected
// context). If it returns the cancellation (an error wrapping ErrCanceled),
// its activities are compensated and it ends canceled; another error ends
// it as a failure (compensated or failed), a result completes it. Once the
// workflow has ended and compensates (or is parked), a cancel is accepted
// but has no effect: compensation always completes (Terminate stops it).
func (e *Engine) Cancel(ctx context.Context, instanceID string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	return e.st.RequestCancel(ctx, instanceID)
}

// RetryCompensation resumes the compensation of instanceID's execution,
// parked as needs_attention because a compensation failed after its
// retries (errors.Is ErrCompensationFailed), once the cause is fixed: the
// failed compensation runs again, then the remaining ones. ErrNotActive
// when the execution is not parked that way.
func (e *Engine) RetryCompensation(ctx context.Context, instanceID string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	return e.st.ReopenCompensation(ctx, instanceID)
}

// Resume resumes the execution of instanceID parked as needs_attention
// because its workflow code was wrong (errors.Is ErrNonDeterministic or
// ErrStuck), once fixed code is deployed: it runs again from its history,
// replayed by the code now registered (parked again if that still does
// not match). ErrNotActive when the execution is not parked that way; a
// failed compensation resumes with RetryCompensation.
func (e *Engine) Resume(ctx context.Context, instanceID string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	return e.st.ReopenParked(ctx, instanceID)
}

// Terminate ends the running execution of instanceID at once, without
// running workflow code or compensation (for stuck executions). A failed
// workflow compensates only once every activity it started has an outcome
// (each is canceled; the outcome tells whether it has a compensator): an
// activity that ignores its context, or whose queue no engine runs, keeps
// it compensating until it ends or the execution is terminated.
func (e *Engine) Terminate(ctx context.Context, instanceID, reason string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	reason = text.Safe(reason, maxErrorText)
	ended, err := e.st.Terminate(ctx, instanceID, reason)
	if err != nil {
		return err
	}
	if e.obs != nil {
		func() {
			defer func() { _ = recover() }() // a broken observer must not fail Terminate
			e.obs.RunEnded(runInfo(ended))
		}()
	}
	return nil
}

// PurgeEnded deletes up to limit executions that ended more than olderThan
// ago (their histories with them), freeing their instance ids. Descendants
// of an active execution (children, grandchildren) are kept until it ends:
// its compensation may reach them.
func (e *Engine) PurgeEnded(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return e.st.PurgeEnded(ctx, olderThan, limit)
}
