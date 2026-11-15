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
	// PollMax bounds the idle polling interval (default 1s; Postgres
	// notifications wake pollers earlier).
	PollMax time.Duration
	// EagerWindow is how long an execution waits for its eager activities
	// before letting signals and timers in (default 200ms).
	EagerWindow time.Duration
}

// Engine runs the workflows and activities defined in this binary against
// a Postgres database. Several processes can run engines on the same
// database: tasks are leased, and every write is fenced.
type Engine struct {
	st store.Store
	w  *worker.Worker
}

// New returns a stopped engine on pool. Run Migrate first.
func New(pool *pgxpool.Pool, opts Options) (*Engine, error) {
	return newEngine(pg.New(pool), opts)
}

// Migrate creates or upgrades the engine's tables (schema "flows"). It is
// safe to run from several processes at once.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error { return pg.Migrate(ctx, pool) }

func newEngine(st store.Store, opts Options) (*Engine, error) {
	if opts.InstanceID == "" {
		host, _ := os.Hostname()
		var b [4]byte
		_, _ = rand.Read(b[:])
		opts.InstanceID = host + "-" + hex.EncodeToString(b[:])
	}
	if len(opts.Queues) == 0 {
		opts.Queues = []string{DefaultQueue}
	}
	w, err := worker.New(st, workerRegistry{}, workerRegistry{}, worker.Options{
		Owner: opts.InstanceID, Queues: opts.Queues, LeaseTTL: opts.LeaseTTL,
		MaxWorkflowTasks: opts.MaxWorkflowTasks, MaxActivities: opts.MaxActivities,
		Eager: !opts.DisableEager, EagerWindow: opts.EagerWindow, StickyFor: opts.StickyFor,
		PollMax: opts.PollMax, CacheSize: opts.CacheSize, Errors: errorCodec{}, Logger: opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("flows: %w", err)
	}
	return &Engine{st: st, w: w}, nil
}

// Start starts running tasks. Without Start, an engine still starts
// workflows and sends signals (for client-only processes).
func (e *Engine) Start(ctx context.Context) error { return e.w.Start(ctx) }

// Stop stops taking tasks and waits for the running ones until ctx is done;
// those still running then are canceled and resumed by another instance.
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
// context is canceled, which cancels its activities, timers and children;
// the workflow decides what to do (it can compensate in a Disconnected
// context).
func (e *Engine) Cancel(ctx context.Context, instanceID string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	return e.st.RequestCancel(ctx, instanceID)
}

// Terminate ends the running execution of instanceID at once, without
// running workflow code or compensation (for stuck executions).
func (e *Engine) Terminate(ctx context.Context, instanceID, reason string) error {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return err
	}
	return e.st.Terminate(ctx, instanceID, text.Safe(reason, maxErrorText))
}

// PurgeEnded deletes up to limit executions that ended more than olderThan
// ago (their histories with them), freeing their instance ids.
func (e *Engine) PurgeEnded(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return e.st.PurgeEnded(ctx, olderThan, limit)
}
