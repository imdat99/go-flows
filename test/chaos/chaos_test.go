//go:build chaos

// Package chaos_test runs crash scenarios against real processes: the test
// binary re-executes itself as a child "instance" that is then killed
// (SIGKILL) or frozen (SIGSTOP), and engines in the parent recover its
// executions.
//
// Ported from saga test/chaos (same author), for go-flows.
package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/testdb"
)

// Child process protocol, passed through the environment.
const (
	envMode  = "FLOWS_CHAOS_CHILD" // hang | pause | many | comp-hang | lock-hang | wf-pause | wf-short | tree | exit | soak
	envDSN   = "FLOWS_CHAOS_DSN"
	envRuns  = "FLOWS_CHAOS_RUNS"
	envTTL   = "FLOWS_CHAOS_TTL" // the child's LeaseTTL (default leaseTTL)
	leaseTTL = time.Second
	// killTTL is the lease of a child that gets killed in a test counting
	// exact invocations: long enough that a renewal late under load (-race,
	// other tests running) does not give a lease up before the kill (a
	// worker gives a lease up after ttl - ttl/3 without renewal) and run a
	// step twice.
	killTTL = "5s"
	// parentTTL is the lease of the parent's engines: they are never meant
	// to lose one, and the tests count their attempts.
	parentTTL = 10 * time.Second
)

// childTTL is the child's lease (envTTL).
var childTTL = leaseTTL

// db is the database of the current test (parent) or of the child process.
// Parent tests do not run in parallel.
var db atomic.Pointer[pgxpool.Pool]

// hangMode makes the reserve activity stall in the child: "hang", "many"
// and "lock-hang" block forever (the process gets killed), "pause" sleeps
// (the process gets frozen meanwhile, then thawed); "comp-hang" stalls a
// compensator instead.
var hangMode string

// gate, when set in the parent, holds the parent's reserve attempts until
// closed (test B: the takeover holds its claim while the frozen child
// writes).
var gate atomic.Pointer[chan struct{}]

// slowSteps, when set in the parent, slows every attempt of the order's
// steps (test C: no single racer can take all the work).
var slowSteps atomic.Int64

// effect is the module side of an activity: it records every attempt, and
// applies the effect once per key (the module's processed_requests).
func effect(ctx context.Context, name string, k flows.Key) error {
	pool := db.Load()
	if _, err := pool.Exec(ctx, `INSERT INTO chaos_attempt (key, step, pid) VALUES ($1, $2, $3)`,
		string(k), name, os.Getpid()); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO chaos_effect (key, step) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		string(k), name)
	return err
}

func defineStep(name string, stall bool) *flows.Activity[int, int, struct{}] {
	return flows.DefineActivity(name, func(ctx context.Context, k flows.Key, v int) (int, struct{}, error) {
		if err := effect(ctx, name, k); err != nil {
			return 0, struct{}{}, err
		}
		if d := time.Duration(slowSteps.Load()); d > 0 {
			time.Sleep(d)
		}
		if stall && hangMode != "" {
			if _, err := db.Load().Exec(ctx, `INSERT INTO chaos_marker (key) VALUES ($1)`, string(k)); err != nil {
				return 0, struct{}{}, err
			}
			switch hangMode {
			case "hang", "many", "lock-hang":
				select {} // killed with SIGKILL while here
			case "pause":
				// Frozen with SIGSTOP meanwhile, for longer than this sleep
				// (until the takeover claims the task: at least the lease
				// left, over two thirds of a TTL): on thaw the attempt
				// returns at once and races the heartbeat's overdue
				// expiry: it writes its stale outcome (fenced off by the
				// store) or the lease is given up first (nothing written).
				time.Sleep(childTTL / 2)
				return v + 1000, struct{}{}, nil // told apart from the takeover's
			}
		}
		if g := gate.Load(); g != nil && stall {
			select {
			case <-*g:
			case <-ctx.Done():
				return 0, struct{}{}, ctx.Err()
			}
		}
		return v + 1, struct{}{}, nil
	}, nil, flows.WithRetry(flows.RetryPolicy{Attempts: 3, Initial: 10 * time.Millisecond}))
}

var (
	validate = defineStep("chaos.validate", false)
	reserve  = defineStep("chaos.reserve", true)
	charge   = defineStep("chaos.charge", false)
	confirm  = defineStep("chaos.confirm", false)

	order = flows.DefineWorkflow("chaos.order", 1, func(c flows.Ctx, in int) (string, error) {
		v := in
		for _, s := range []*flows.Activity[int, int, struct{}]{validate, reserve, charge, confirm} {
			var err error
			if v, err = s.Run(c, v); err != nil {
				return "", err
			}
		}
		return strconv.Itoa(v), nil
	})
)

// refund's compensator stalls in the child in mode "comp-hang" (killed
// while undoing); the saga fails after it.
var (
	refund = flows.DefineActivity("chaos.refund", func(ctx context.Context, k flows.Key, v int) (int, string, error) {
		return v, "refund-of-" + string(k), effect(ctx, "chaos.refund", k)
	}, func(ctx context.Context, k flows.Key, data string) error {
		if data != "refund-of-"+string(k) {
			return flows.Permanent(fmt.Errorf("compensation data %q lost", data))
		}
		if err := effect(ctx, "chaos.undo-refund", flows.Key(string(k)+"/undo")); err != nil {
			return err
		}
		if hangMode == "comp-hang" {
			if _, err := db.Load().Exec(ctx, `INSERT INTO chaos_marker (key) VALUES ($1)`, string(k)); err != nil {
				return err
			}
			select {} // killed while compensating
		}
		return nil
	})
	errDeclined = errors.New("declined")
	failing     = flows.DefineWorkflow("chaos.failing", 1, func(c flows.Ctx, in int) (string, error) {
		if _, err := refund.Run(c, in); err != nil {
			return "", err
		}
		return "", errDeclined
	})
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(envMode); mode != "" {
		os.Exit(child(mode))
	}
	os.Exit(m.Run())
}

// child starts its executions and runs them until killed; for "pause" it
// reports how they ended on stdout.
func child(mode string) int {
	if mode == "soak" {
		return childSoak()
	}
	hangMode = mode
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv(envDSN))
	if err != nil {
		fmt.Println("RESULT: connect:", err)
		return 2
	}
	db.Store(pool)
	ttl := leaseTTL
	if v := os.Getenv(envTTL); v != "" {
		if ttl, err = time.ParseDuration(v); err != nil {
			fmt.Println("RESULT: ttl:", err)
			return 2
		}
	}
	childTTL = ttl
	opts := flows.Options{
		InstanceID: "child", LeaseTTL: ttl, PollMax: 50 * time.Millisecond, Logger: quietLogger(os.Stderr),
		Observer: printAttempts{},
	}
	if mode == "wf-pause" || mode == "wf-short" || mode == "tree" {
		// Workflow code waits on the test (wf-*): never handed back as stuck.
		opts.StuckTimeout = 5 * time.Minute
		opts.Tracer = printTasks{}
	}
	if mode == "pause" {
		// ACTEND ... lost=true when the attempt's lease was given up
		// before its outcome was written (TestFrozenInstanceIsFencedOff).
		opts.Tracer = printTasks{}
	}
	eng, err := flows.New(pool, opts)
	if err != nil {
		fmt.Println("RESULT: engine:", err)
		return 2
	}
	if err := eng.Start(ctx); err != nil {
		fmt.Println("RESULT: start:", err)
		return 2
	}
	switch mode {
	case "lock-hang":
		_, err := locked.Start(ctx, eng, "locked-0", 0)
		if err == nil {
			select {} // killed while holding the lock
		}
		fmt.Printf("RESULT: %v\n", err)
	case "comp-hang":
		h, err := failing.Start(ctx, eng, "failing-0", 1)
		if err == nil {
			_, err = h.Wait(ctx) // never returns: killed while compensating
		}
		fmt.Printf("RESULT: %v\n", err)
	case "wf-pause", "wf-short":
		childFenced(ctx, eng)
	case "tree":
		childTrees(ctx, eng)
	case "exit":
		childExit(ctx, eng)
	default:
		childOrders(ctx, eng)
	}
	return 0
}

func childOrders(ctx context.Context, eng *flows.Engine) {
	n, _ := strconv.Atoi(os.Getenv(envRuns))
	var errs []error
	var handles []*flows.Handle[string]
	for i := range max(n, 1) {
		h, err := order.Start(ctx, eng, "order-"+strconv.Itoa(i), i*100)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		wctx, cancel := context.WithTimeout(ctx, time.Minute)
		_, err := h.Wait(wctx)
		cancel()
		errs = append(errs, err)
	}
	fmt.Printf("RESULT: %v\n", errors.Join(errs...))
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = eng.Stop(sctx)
}

// printAttempts prints the attempts the child is about to record (the
// worker reports an attempt only when it did not see its lease lost).
type printAttempts struct{}

func (printAttempts) ActivityAttempt(a flows.ActivityAttempt) {
	fmt.Printf("ATTEMPT %s %s\n", a.Activity, a.Outcome)
}

func (printAttempts) RunEnded(flows.RunInfo) {}

func quietLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE chaos_attempt (key text NOT NULL, step text NOT NULL, pid int NOT NULL);
		CREATE TABLE chaos_effect (key text PRIMARY KEY, step text NOT NULL);
		CREATE TABLE chaos_marker (key text PRIMARY KEY);
		CREATE TABLE chaos_go (key text PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	db.Store(pool)
	t.Cleanup(func() {
		if t.Failed() {
			dumpState(t, pool)
		}
	})
	return pool
}

// syncBuffer is a buffer the child's output is copied into while the test
// reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func spawn(t *testing.T, pool *pgxpool.Pool, mode string, runs int, env ...string) (*exec.Cmd, *syncBuffer) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		envMode+"="+mode, envDSN+"="+testdb.DSN(pool), envRuns+"="+strconv.Itoa(runs))
	cmd.Env = append(cmd.Env, env...)
	var out syncBuffer
	cmd.Stdout = &out
	cmd.Stderr = &out                                                  // its logs too: printed when the test fails
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL} // never outlive the test
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("child %s (pid %d, mode %s) output:\n%s", strings.Join(env, " "), cmd.Process.Pid, mode, out.String())
		}
	})
	return cmd, &out
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			// The state now: the dump at cleanup comes after the engines
			// stopped (which releases their leases).
			t.Logf("timed out waiting for %s; state at the timeout:", what)
			dumpState(t, db.Load())
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func startEngine(t *testing.T, pool *pgxpool.Pool, id string) *flows.Engine {
	t.Helper()
	return startEngineWith(t, pool, flows.Options{InstanceID: id})
}

// startEngineWith starts an engine with the suite's polling, a long
// lease (parentTTL) unless opts has one, and opts' other fields.
func startEngineWith(t *testing.T, pool *pgxpool.Pool, opts flows.Options) *flows.Engine {
	t.Helper()
	if opts.LeaseTTL == 0 {
		opts.LeaseTTL = parentTTL
	}
	opts.PollMax, opts.Logger = 50*time.Millisecond, quietLogger(t.Output())
	eng, err := flows.New(pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return eng
}

func waitDone(t *testing.T, eng *flows.Engine, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := order.Handle(eng, id).Wait(ctx)
	if err != nil {
		t.Fatalf("execution %s: %v", id, err)
	}
	return out
}

// assertOnce checks that every activity of every execution took effect
// exactly once, that the activities around the interrupted one ran exactly
// once, and that each history records each activity's outcome once.
func assertOnce(t *testing.T, pool *pgxpool.Pool, runs int) {
	t.Helper()
	if n := count(t, pool, `SELECT count(*) FROM chaos_effect`); n != 4*runs {
		t.Fatalf("effects = %d, want %d", n, 4*runs)
	}
	for _, step := range []string{"chaos.validate", "chaos.charge", "chaos.confirm"} {
		if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = $1`, step); n != runs {
			t.Fatalf("%s invoked %d times, want %d", step, n, runs)
		}
	}
	// reserve: once in the dead instance, once after recovery, same key.
	if n := count(t, pool, `SELECT count(DISTINCT key) FROM chaos_attempt WHERE step = 'chaos.reserve'`); n != runs {
		t.Fatalf("reserve used %d distinct keys, want %d", n, runs)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.reserve'`); n != 2*runs {
		t.Fatalf("reserve invoked %d times, want %d", n, 2*runs)
	}
	// The second reserve attempt ran in this (the recovering) process.
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.reserve' AND pid = $1`, os.Getpid()); n != runs {
		t.Fatalf("reserve recovered in this process %d times, want %d", n, runs)
	}
	// One recorded outcome per activity, one end.
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.ActivityCompleted)); n != 4*runs {
		t.Fatalf("recorded activity outcomes = %d, want %d", n, 4*runs)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE status = 'completed'`); n != runs {
		t.Fatalf("completed executions = %d, want %d", n, runs)
	}
}

// A: an instance is killed mid-activity; another recovers the execution and
// runs the activity again with the same key, so its effect happens once.
func TestKilledInstanceIsRecovered(t *testing.T) {
	pool := setup(t)
	cmd, _ := spawn(t, pool, "hang", 1, envTTL+"="+killTTL)
	waitFor(t, "child to enter reserve", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	// Killed while holding the activity's lease (eager: and the workflow's).
	if n := count(t, pool, `SELECT count(*) FROM flows.activity_task WHERE owner_id = 'child' AND locked_until > now()`); n != 1 {
		t.Fatalf("activity leases held by the child: %d", n)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	eng := startEngine(t, pool, "recover")
	if out := waitDone(t, eng, "order-0"); out != "4" {
		t.Fatalf("output = %q", out)
	}
	assertOnce(t, pool, 1)
}

// B: an instance freezes past its leases (a GC pause); another takes the
// activity over and holds it (its attempt waits on a gate). The frozen one
// wakes while the takeover still runs: its attempt returns at once and
// races its heartbeat's overdue expiry. Either it writes its late outcome
// and the store's fence (owner and lease version) rejects it, or it gives
// the lease up first and writes nothing. Either way the takeover's outcome
// is the one recorded.
func TestFrozenInstanceIsFencedOff(t *testing.T) {
	pool := setup(t)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	gate.Store(&release)
	t.Cleanup(func() { open(); gate.Store(nil) })
	cmd, out := spawn(t, pool, "pause", 1, envTTL+"=3s")
	waitFor(t, "child to enter reserve", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	eng := startEngine(t, pool, "takeover")
	waitFor(t, "the takeover to hold the reserve task", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.reserve' AND pid = $1`, os.Getpid()) == 1 &&
			count(t, pool, `SELECT count(*) FROM flows.activity_task WHERE name = 'chaos.reserve' AND owner_id = 'takeover'`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	// On thaw two timers are overdue in the child: its attempt's sleep and
	// its heartbeat's expiry (T11.1, which needs no renewal round trip). If
	// the attempt returns first, the child writes its stale outcome and the
	// store's fence rejects it (an attempt is reported only when its worker
	// did not see the lease lost first); if the heartbeat fires first, the
	// child gives the lease up and writes nothing. Both are safe; what must
	// never happen is checked below either way.
	var wrote bool
	waitFor(t, "the thawed child to write its outcome or give its lease up", 15*time.Second, func() bool {
		wrote = strings.Contains(out.String(), "ATTEMPT chaos.reserve completed")
		return wrote || strings.Contains(out.String(), "ACTEND order-0 chaos.reserve lost=true")
	})
	if wrote {
		time.Sleep(200 * time.Millisecond) // the write went to the store
		t.Log("the thawed child wrote its stale outcome (fenced off)")
	} else {
		t.Log("the thawed child gave its lease up before writing")
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.pending_event WHERE type = $1`, int(history.ActivityCompleted)); n != 0 {
		t.Fatalf("a stale outcome was delivered (%d pending)", n)
	}
	open()
	if got := waitDone(t, eng, "order-0"); got != "4" {
		t.Fatalf("output %q: the stale outcome (1000+) was recorded", got)
	}
	_ = cmd.Wait()
	assertOnce(t, pool, 1)
}

// C: three instances race to recover the executions of a killed one; each
// activity takes effect once and each execution completes once. Each racer
// is bounded (two workflow tasks, two activities at a time) and the steps
// are slowed, so that no racer can take all the work; the racers' counts
// prove that more than one took part.
func TestThreeInstancesRaceRecovery(t *testing.T) {
	const runs = 30
	pool := setup(t)
	cmd, _ := spawn(t, pool, "many", runs, envTTL+"="+killTTL)
	waitFor(t, "child to enter reserve in every execution", 60*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == runs
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	slowSteps.Store(int64(30 * time.Millisecond))
	t.Cleanup(func() { slowSteps.Store(0) })
	engines := make([]*flows.Engine, 3)
	stats := make([]*racerStats, 3)
	for i := range engines {
		p, err := pgxpool.New(context.Background(), testdb.DSN(pool))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		stats[i] = &racerStats{name: "racer-" + strconv.Itoa(i)}
		engines[i] = startEngineWith(t, p, flows.Options{
			InstanceID: stats[i].name, MaxWorkflowTasks: 2, MaxActivities: 2,
			Observer: stats[i], Tracer: stats[i],
		})
	}
	for i := range runs {
		waitDone(t, engines[i%3], "order-"+strconv.Itoa(i))
	}
	assertOnce(t, pool, runs)
	var attempted, ended, attempts, ends int64
	for _, s := range stats {
		t.Log(s)
		a, e := s.attempts.Load(), s.ended.Load()
		attempts, ends = attempts+a, ends+e
		if a > 0 {
			attempted++
		}
		if e > 0 {
			ended++
		}
	}
	// Every recovered attempt (reserve, charge, confirm) and every end was
	// counted by some racer.
	if attempts < 3*runs || ends != runs {
		t.Fatalf("racers counted %d completed attempts (want >= %d) and %d ends (want %d)", attempts, 3*runs, ends, runs)
	}
	if attempted < 2 || ended < 2 {
		t.Fatalf("only %d racers completed attempts and %d ended executions: the race had one racer", attempted, ended)
	}
}

// D: an instance is killed while a compensator runs; another resumes the
// compensation and runs the compensator again with the same key: the undo
// takes effect once, and the execution ends compensated.
func TestKilledDuringCompensation(t *testing.T) {
	pool := setup(t)
	cmd, _ := spawn(t, pool, "comp-hang", 1, envTTL+"="+killTTL)
	waitFor(t, "child to enter the compensator", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	eng := startEngine(t, pool, "recover-comp")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := failing.Handle(eng, "failing-0").Wait(ctx)
	var re *flows.RunError
	if !errors.As(err, &re) || re.Status != flows.StatusCompensated {
		t.Fatalf("execution: %v", err)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.undo-refund'`); n != 2 {
		t.Fatalf("compensator invoked %d times, want 2 (killed, recovered)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_effect WHERE step = 'chaos.undo-refund'`); n != 1 {
		t.Fatalf("undo took effect %d times, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.refund'`); n != 1 {
		t.Fatalf("activity invoked %d times, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.undo-refund' AND pid = $1`, os.Getpid()); n != 1 {
		t.Fatalf("compensation recovered in this process %d times, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.CompensationCompleted)); n != 1 {
		t.Fatalf("recorded compensations = %d, want 1", n)
	}
}

var (
	critical = flows.DefineActivity("chaos.critical", func(ctx context.Context, k flows.Key, _ int) (int, struct{}, error) {
		if err := effect(ctx, "chaos.critical", k); err != nil {
			return 0, struct{}{}, err
		}
		if hangMode == "lock-hang" {
			if _, err := db.Load().Exec(ctx, `INSERT INTO chaos_marker (key) VALUES ($1)`, string(k)); err != nil {
				return 0, struct{}{}, err
			}
			select {} // killed holding the lock
		}
		_, err := db.Load().Exec(ctx, `INSERT INTO chaos_section (key, at) VALUES ($1, clock_timestamp())`, string(k))
		return 0, struct{}{}, err
	}, nil)
	locked = flows.DefineWorkflow("chaos.locked", 1, func(c flows.Ctx, _ int) (int, error) {
		if err := flows.Lock(c, "chaos", 10*time.Second, flows.LockWait(flows.Exponential(200, 20*time.Millisecond, 200*time.Millisecond))); err != nil {
			return 0, err
		}
		return critical.Run(c, 0)
	})
)

// E: an instance is killed holding a business lock. The recovering engine
// takes the execution over with its lock (no other execution gets it
// meanwhile, though the lock lapses on the dead one's lease), and the
// waiting execution gets the lock only after the first ended.
func TestKilledHoldingALock(t *testing.T) {
	pool := setup(t)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE chaos_section (key text PRIMARY KEY, at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	cmd, _ := spawn(t, pool, "lock-hang", 1, envTTL+"="+killTTL)
	waitFor(t, "child to enter the locked section", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	eng := startEngine(t, pool, "recover-lock")
	if _, err := locked.Start(context.Background(), eng, "locked-1", 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, id := range []string{"locked-0", "locked-1"} {
		if _, err := locked.Handle(eng, id).Wait(ctx); err != nil {
			t.Fatalf("%s: %v", id, err)
		}
	}
	// The recovered holder finished its section before the waiter entered.
	if n := count(t, pool, `
		SELECT count(*) FROM chaos_section a, chaos_section b
		WHERE a.key LIKE 'locked-0%' AND b.key LIKE 'locked-1%' AND a.at < b.at`); n != 1 {
		t.Fatal("the waiter entered the section before the recovered holder")
	}
}
