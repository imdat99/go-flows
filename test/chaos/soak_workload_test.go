//go:build chaos

package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
)

// The workload of the database-side chaos tests (T11.7) and of the soak:
// orders that receive a signal, run activities failing at random, sleep on
// durable timers, run children (one of them under a business lock),
// continue as new, and for some end declined and compensated. Their child
// processes ("soak" mode) run engines whose connections go through a proxy
// the parent test freezes; the activities record their attempts (start,
// end, outcome) and their effects (once per key) through a separate pool,
// straight to the server.

const (
	envAppDSN   = "FLOWS_CHAOS_APP_DSN"  // the activities' pool, not proxied
	envInstance = "FLOWS_CHAOS_INSTANCE" // the child engine's InstanceID
	envSlow     = "FLOWS_CHAOS_SLOW"     // soak.step lasts longer than the lease
	envSoak     = "FLOWS_CHAOS_SOAK"     // the soak's retry policy and MaxTaskClaims
	// soakStopDeadline is the deadline of the Stop a child runs on SIGTERM.
	soakStopDeadline = 2 * time.Second
	soakRounds       = 2
)

var (
	soakApp  atomic.Pointer[pgxpool.Pool]
	soakSlow atomic.Bool

	errSoakTransient = errors.New("soak: transient failure")
)

// soakSchema is the module side of the workload: every attempt (local
// wall-clock start and end, the processes share the host's clock with the
// server), and the effects, once per key.
const soakSchema = `
	CREATE TABLE soak_attempt (
		id       bigint PRIMARY KEY, -- chosen by the attempt
		key      text NOT NULL,
		step     text NOT NULL,
		lock_key text NOT NULL DEFAULT '',
		pid      int NOT NULL,
		started  timestamptz NOT NULL,
		ended    timestamptz,
		outcome  text NOT NULL DEFAULT '',
		dup      boolean NOT NULL DEFAULT false,
		attempt  int NOT NULL DEFAULT 0 -- the engine's attempt number (ActivityInfo)
	);
	CREATE INDEX ON soak_attempt (pid) WHERE ended IS NULL;
	CREATE TABLE soak_effect (key text PRIMARY KEY, step text NOT NULL);`

// soakAttempt is an attempt of step with key k: recorded, failing at
// random with probability fail before its effect, applying its effect
// (once per key) in the middle of its work, which honors ctx.
func soakAttempt(ctx context.Context, step, k, lock string, fail float64) (err error) {
	// The id is chosen here, and the end is an upsert: an attempt whose
	// start was not recorded (its insert canceled, or its answer lost to a
	// connection cut, the insert committing after the end) still gets its
	// end, kept by the late insert.
	a := attemptRec{
		id: rand.Int64(), key: k, step: step, lock: lock, pid: os.Getpid(), started: time.Now(), attempt: attemptNumber(ctx),
	}
	dup := false
	defer func() { a.end(attemptOutcome(ctx, err), dup) }()
	if _, err := appExec(ctx, `
		INSERT INTO soak_attempt (id, key, step, lock_key, pid, started, attempt) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING`,
		a.id, a.key, a.step, a.lock, a.pid, a.started, a.attempt); err != nil {
		return err
	}
	if rand.Float64() < fail {
		return errSoakTransient
	}
	lo, hi := 10*time.Millisecond, 150*time.Millisecond
	if step == "soak.step" && soakSlow.Load() {
		// Longer than the give-up after a blackhole (ttl - ttl/3 after
		// the last renewal, at most 2s with soakTTL) and than the lease.
		lo, hi = 4*time.Second, 6*time.Second
	}
	d := lo + rand.N(hi-lo)
	if err := sleepCtx(ctx, d/2); err != nil {
		return err
	}
	n, err := appExec(ctx, `INSERT INTO soak_effect (key, step) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`, k, step)
	if err != nil {
		return err
	}
	dup = n == 0 // or an insert whose answer was lost, retried
	return sleepCtx(ctx, d-d/2)
}

// appExec runs an idempotent statement of the module, retrying through
// connection cuts: the pool's idle connections all die at once when the
// backends are terminated, and the attempts of an activity failing on
// them one after another would exhaust its retry policy (the module's own
// fault, not the engine's). It returns the rows affected.
func appExec(ctx context.Context, sql string, args ...any) (int64, error) {
	for try := 1; ; try++ {
		tag, err := soakApp.Load().Exec(ctx, sql, args...)
		if err == nil || ctx.Err() != nil || try == 10 {
			return tag.RowsAffected(), err
		}
		if err := sleepCtx(ctx, 50*time.Millisecond); err != nil {
			return 0, err
		}
	}
}

func attemptOutcome(ctx context.Context, err error) string {
	switch {
	case err == nil:
		return "ok"
	case ctx.Err() != nil:
		if cause := context.Cause(ctx); errors.Is(cause, flows.ErrLeaseLost) {
			return "lease-lost"
		} else if cause != nil {
			return "canceled: " + cause.Error()
		}
		return "canceled"
	case errors.Is(err, errSoakTransient):
		return "fail"
	default:
		return "error: " + err.Error()
	}
}

// attemptRec is an attempt as soak_attempt records it.
type attemptRec struct {
	id              int64
	key, step, lock string
	pid, attempt    int
	started         time.Time
}

// end records the end of the attempt, its time taken now; it retries for
// a while (connections cut).
func (a attemptRec) end(outcome string, dup bool) {
	ended := time.Now()
	for range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := soakApp.Load().Exec(ctx, `
			INSERT INTO soak_attempt (id, key, step, lock_key, pid, started, attempt, ended, outcome, dup)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (id) DO UPDATE SET ended = excluded.ended, outcome = excluded.outcome, dup = excluded.dup`,
			a.id, a.key, a.step, a.lock, a.pid, a.started, a.attempt, ended, outcome, dup)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// soakRetry is the activities' retry policy. Attempts lost to faults
// count toward its limit (T11.3, ErrAttemptsExhausted), on top of the
// random failures (p <= 0.15): the short tests lose an attempt or two per
// activity at most; the soak's faults, one after another, could hit one
// activity's engines many times.
var soakRetry = flows.Exponential(soakRetryAttempts(), 20*time.Millisecond, 500*time.Millisecond)

func soakRetryAttempts() int {
	if os.Getenv(envSoak) != "" {
		return 40
	}
	return 15
}

// soakMaxTaskClaims is the children's MaxTaskClaims: the default (10) in
// the short tests; in the soak, a task loses its claim to each fault that
// hits its engine, and the faults could hit one execution's engines ten
// times in a row (the bound stays).
func soakMaxTaskClaims() int {
	if os.Getenv(envSoak) != "" {
		return 50
	}
	return 0
}

type attemptKey struct{}

// attemptNumber is the attempt number soakTracer put in ctx.
func attemptNumber(ctx context.Context) int {
	n, _ := ctx.Value(attemptKey{}).(int)
	return n
}

var (
	soakStep = flows.DefineActivity("soak.step", func(ctx context.Context, k flows.Key, v int) (int, string, error) {
		return v + 1, "undo:" + string(k), soakAttempt(ctx, "soak.step", string(k), "", 0.15)
	}, func(ctx context.Context, k flows.Key, data string) error {
		if data != "undo:"+string(k) {
			return flows.Permanent(fmt.Errorf("compensation data %q lost", data))
		}
		return soakAttempt(ctx, "soak.undo", string(k)+"/undo", "", 0.1)
	}, flows.WithRetry(soakRetry), flows.WithCompensateRetry(soakRetry))

	soakCritical = flows.DefineActivity("soak.critical", func(ctx context.Context, k flows.Key, lock string) (int, struct{}, error) {
		return 0, struct{}{}, soakAttempt(ctx, "soak.critical", string(k), lock, 0.1)
	}, nil, flows.WithRetry(soakRetry))

	soakDecline = flows.DefineActivity("soak.decline", func(ctx context.Context, k flows.Key, _ int) (int, struct{}, error) {
		if err := soakAttempt(ctx, "soak.decline", string(k), "", 0); err != nil {
			return 0, struct{}{}, err
		}
		return 0, struct{}{}, flows.Permanent(errDeclined)
	}, nil, flows.WithRetry(soakRetry))

	soakLeaf = flows.DefineWorkflow("soak.leaf", 1, func(c flows.Ctx, v int) (int, error) {
		x, err := soakStep.Run(c, v)
		if err != nil {
			return 0, err
		}
		if err := timed(c, 50*time.Millisecond); err != nil {
			return 0, err
		}
		return 2 * x, nil
	})

	soakLocked = flows.DefineWorkflow("soak.locked", 1, func(c flows.Ctx, id int) (int, error) {
		key := soakLockKey(id)
		if err := flows.Lock(c, key, 30*time.Second, flows.LockWait(flows.Exponential(100000, 20*time.Millisecond, 300*time.Millisecond))); err != nil {
			return 0, err
		}
		return soakCritical.Run(c, key)
	})

	soakOrder = flows.DefineWorkflow("soak.order", 1, func(c flows.Ctx, in treeIn) (int, error) {
		acc := in.Acc
		if in.Round == 0 && soakSignaled(in.ID) {
			v, err := flows.Signals[int](c, "bump").Receive(c)
			if err != nil {
				return 0, err
			}
			acc += v
		}
		v, err := soakStep.Run(c, in.ID*100+in.Round)
		if err != nil {
			return 0, err
		}
		if err := timed(c, time.Duration(50+(in.ID+in.Round)%5*60)*time.Millisecond); err != nil {
			return 0, err
		}
		prefix := soakID(in.ID) + "/r" + strconv.Itoa(in.Round)
		leaf := soakLeaf.Child(c, prefix+"/leaf", v)
		if in.Round == 0 && soakLocks(in.ID) {
			if _, err := soakLocked.Child(c, prefix+"/locked", in.ID).Get(c); err != nil {
				return 0, err
			}
		}
		x, err := leaf.Get(c)
		if err != nil {
			return 0, err
		}
		acc += x
		if in.Round < soakRounds-1 {
			return 0, flows.ContinueAsNew(c, treeIn{ID: in.ID, Round: in.Round + 1, Acc: acc})
		}
		if soakDeclines(in.ID) {
			_, err := soakDecline.Run(c, acc)
			return 0, err
		}
		return acc, nil
	})
)

func soakID(id int) string        { return "order-" + strconv.Itoa(id) }
func soakSignaled(id int) bool    { return id%2 == 0 }
func soakBump(id int) int         { return id + 7 }
func soakDeclines(id int) bool    { return id%4 == 1 }
func soakLocks(id int) bool       { return id%3 == 0 && !soakDeclines(id) }
func soakLockKey(id int) string   { return "soak-lock-" + strconv.Itoa(id/3%3) }
func soakLeafID(id, r int) string { return soakID(id) + "/r" + strconv.Itoa(r) + "/leaf" }
func soakLockedID(id int) string  { return soakID(id) + "/r0/locked" }
func soakFinalStatus(id int) string {
	if soakDeclines(id) {
		return "compensated"
	}
	return "completed"
}

// soakWant is the output of an order that is not declined.
func soakWant(id int) int {
	acc := 0
	if soakSignaled(id) {
		acc += soakBump(id)
	}
	for r := range soakRounds {
		v := id*100 + r + 1
		acc += 2 * (v + 1)
	}
	return acc
}

// soakTracer prints, in the child, the tasks and attempts that ended with
// their lease lost.
type soakTracer struct{}

func (soakTracer) StartWorkflow(context.Context, flows.StartInfo) ([]byte, func(bool, error)) {
	return nil, func(bool, error) {}
}

func (soakTracer) StartActivity(ctx context.Context, a flows.ActivityInfo) (context.Context, func(error)) {
	return context.WithValue(ctx, attemptKey{}, a.Attempt), func(err error) {
		if errors.Is(err, flows.ErrLeaseLost) {
			fmt.Printf("LOST activity %s %s\n", a.InstanceID, a.Activity)
		}
	}
}

func (soakTracer) StartWorkflowTask(ctx context.Context, w flows.WorkflowTaskInfo) (context.Context, func(flows.Status, error)) {
	return ctx, func(_ flows.Status, err error) {
		if errors.Is(err, flows.ErrLeaseLost) {
			fmt.Printf("LOST task %s\n", w.InstanceID)
		}
	}
}

// childSoak runs an engine (connections through the parent's proxy, envDSN)
// until SIGTERM, then stops it with a deadline of soakStopDeadline and
// prints how long Stop took ("STOPPED <ms> <err>").
func childSoak() int {
	ctx := context.Background()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	ttl, err := time.ParseDuration(os.Getenv(envTTL))
	if err != nil {
		fmt.Println("RESULT: ttl:", err)
		return 2
	}
	appCfg, err := pgxpool.ParseConfig(os.Getenv(envAppDSN))
	if err != nil {
		fmt.Println("RESULT: app dsn:", err)
		return 2
	}
	appCfg.ConnConfig.RuntimeParams["statement_timeout"] = "0" // the module is not the subject
	app, err := pgxpool.NewWithConfig(ctx, appCfg)
	if err != nil {
		fmt.Println("RESULT: app pool:", err)
		return 2
	}
	soakApp.Store(app)
	soakSlow.Store(os.Getenv(envSlow) != "")
	pool, err := pgxpool.New(ctx, os.Getenv(envDSN))
	if err != nil {
		fmt.Println("RESULT: pool:", err)
		return 2
	}
	eng, err := flows.New(pool, flows.Options{
		InstanceID: os.Getenv(envInstance), LeaseTTL: ttl, PollMax: 100 * time.Millisecond,
		MaxTaskClaims: soakMaxTaskClaims(),
		Logger:        quietLogger(os.Stderr), Tracer: soakTracer{},
	})
	if err != nil {
		fmt.Println("RESULT: engine:", err)
		return 2
	}
	if err := eng.Start(ctx); err != nil {
		fmt.Println("RESULT: start:", err)
		return 2
	}
	fmt.Println("READY")
	<-sig
	t0 := time.Now()
	sctx, cancel := context.WithTimeout(ctx, soakStopDeadline)
	defer cancel()
	err = eng.Stop(sctx)
	fmt.Printf("STOPPED %d %v\n", time.Since(t0).Milliseconds(), err)
	return 0 // pools not closed: a frozen one would wait for its connections
}
