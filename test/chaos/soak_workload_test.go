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
		dup      boolean NOT NULL DEFAULT false
	);
	CREATE INDEX ON soak_attempt (pid) WHERE ended IS NULL;
	CREATE TABLE soak_effect (key text PRIMARY KEY, step text NOT NULL);`

// soakAttempt is an attempt of step with key k: recorded, failing at
// random with probability fail before its effect, applying its effect
// (once per key) in the middle of its work, which honors ctx.
func soakAttempt(ctx context.Context, step, k, lock string, fail float64) (err error) {
	app := soakApp.Load()
	// The id is chosen here: an insert committed whose answer was lost (a
	// connection cut) still gets its end recorded.
	id := rand.Int64()
	dup := false
	defer func() { endAttempt(id, attemptOutcome(ctx, err), dup) }()
	if _, err := app.Exec(ctx, `
		INSERT INTO soak_attempt (id, key, step, lock_key, pid, started) VALUES ($1, $2, $3, $4, $5, $6)`,
		id, k, step, lock, os.Getpid(), time.Now()); err != nil {
		return err
	}
	if rand.Float64() < fail {
		return errSoakTransient
	}
	lo, hi := 10*time.Millisecond, 150*time.Millisecond
	if step == "soak.step" && soakSlow.Load() {
		lo, hi = 2500*time.Millisecond, 4*time.Second
	}
	d := lo + rand.N(hi-lo)
	if err := sleepCtx(ctx, d/2); err != nil {
		return err
	}
	tag, err := app.Exec(ctx, `INSERT INTO soak_effect (key, step) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`, k, step)
	if err != nil {
		return err
	}
	dup = tag.RowsAffected() == 0
	return sleepCtx(ctx, d-d/2)
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

// endAttempt records the end of attempt id, its time taken now; it retries
// for a while (connections cut).
func endAttempt(id int64, outcome string, dup bool) {
	ended := time.Now()
	for range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := soakApp.Load().Exec(ctx, `UPDATE soak_attempt SET ended = $2, outcome = $3, dup = $4 WHERE id = $1`,
			id, ended, outcome, dup)
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

// soakRetry: attempts lost to faults count toward the limit (T11.3,
// ErrAttemptsExhausted), on top of the random failures (p <= 0.15).
var soakRetry = flows.Exponential(40, 20*time.Millisecond, 500*time.Millisecond)

// soakMaxTaskClaims is the children's MaxTaskClaims (default 10).
const soakMaxTaskClaims = 50

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
	return ctx, func(err error) {
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
		// A task loses its claim to each fault that hits its engine
		// (T11.3 counts them): the soak's faults, one after another, could
		// hit one execution's engines ten times in a row. The bound stays.
		MaxTaskClaims: soakMaxTaskClaims,
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
