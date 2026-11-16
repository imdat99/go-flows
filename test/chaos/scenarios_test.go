//go:build chaos

package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	"github.com/imdat99/go-flows/internal/store"
)

// racerStats counts, for one engine, the workflow tasks it ran (and how
// they ended), the attempts it completed and the executions it ended. It
// is the engine's Observer and Tracer.
type racerStats struct {
	name                          string
	tasks, taskOK, taskLost       atomic.Int64
	attempts, attemptsLost, ended atomic.Int64
}

func (r *racerStats) String() string {
	return fmt.Sprintf("%s: workflow tasks %d (ok %d, lease lost %d), completed attempts %d (lease lost %d), ended %d",
		r.name, r.tasks.Load(), r.taskOK.Load(), r.taskLost.Load(), r.attempts.Load(), r.attemptsLost.Load(), r.ended.Load())
}

func (r *racerStats) ActivityAttempt(a flows.ActivityAttempt) {
	if a.Outcome == "completed" {
		r.attempts.Add(1)
	}
}

func (r *racerStats) RunEnded(flows.RunInfo) { r.ended.Add(1) }

func (r *racerStats) StartWorkflow(context.Context, flows.StartInfo) ([]byte, func(bool, error)) {
	return nil, func(bool, error) {}
}

func (r *racerStats) StartActivity(ctx context.Context, _ flows.ActivityInfo) (context.Context, func(error)) {
	return ctx, func(err error) {
		if errors.Is(err, flows.ErrLeaseLost) {
			r.attemptsLost.Add(1)
		}
	}
}

func (r *racerStats) StartWorkflowTask(ctx context.Context, _ flows.WorkflowTaskInfo) (context.Context, func(flows.Status, error)) {
	r.tasks.Add(1)
	return ctx, func(_ flows.Status, err error) {
		switch {
		case err == nil:
			r.taskOK.Add(1)
		case errors.Is(err, flows.ErrLeaseLost):
			r.taskLost.Add(1)
		}
	}
}

// printTasks prints, in the child, how its workflow tasks ended
// ("TASKOK <instance>", or "TASKEND <instance> lost=<bool> invalid=<bool>
// err=<err>", invalid for a completion the store refused past the lease
// fence: a sequence or schedule conflict) and
// the activity attempts that failed ("ACTEND ..."): the parent reads the
// fence's rejections, or their absence, there.
type printTasks struct{}

func (printTasks) StartWorkflow(context.Context, flows.StartInfo) ([]byte, func(bool, error)) {
	return nil, func(bool, error) {}
}

func (printTasks) StartActivity(ctx context.Context, a flows.ActivityInfo) (context.Context, func(error)) {
	return ctx, func(err error) {
		if err != nil {
			fmt.Printf("ACTEND %s %s lost=%t err=%v\n", a.InstanceID, a.Activity, errors.Is(err, flows.ErrLeaseLost), err)
		}
	}
}

func (printTasks) StartWorkflowTask(ctx context.Context, w flows.WorkflowTaskInfo) (context.Context, func(flows.Status, error)) {
	return ctx, func(_ flows.Status, err error) {
		if err != nil {
			fmt.Printf("TASKEND %s lost=%t invalid=%t err=%v\n", w.InstanceID,
				errors.Is(err, flows.ErrLeaseLost), errors.Is(err, store.ErrInvalidCompletion), err)
		} else {
			fmt.Printf("TASKOK %s\n", w.InstanceID)
		}
	}
}

// dumpState logs the final database state of a failed test.
func dumpState(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dump := func(title, query string) {
		rows, err := pool.Query(ctx, query)
		if err != nil {
			t.Logf("%s: %v", title, err)
			return
		}
		defer rows.Close()
		var b strings.Builder
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Logf("%s: %v", title, err)
				return
			}
			fmt.Fprintln(&b, vals...)
		}
		t.Logf("%s:\n%s", title, b.String())
	}
	dump("executions (id instance workflow status owner lease_version leased last_seq err)", `
		SELECT id, instance_id, workflow, status, owner_id, lease_version, coalesce(locked_until > now(), false), last_seq, err
		FROM flows.execution ORDER BY id`)
	dump("activity tasks (id execution name compensate attempt owner lease_version leased)", `
		SELECT id, execution_id, name, compensate, attempt, owner_id, lease_version, coalesce(locked_until > now(), false)
		FROM flows.activity_task ORDER BY id`)
	dump("pending events (execution type schedule name)", `
		SELECT execution_id, type, schedule_id, name FROM flows.pending_event ORDER BY id`)
	dump("history counts (type count)", `SELECT type, count(*) FROM flows.history GROUP BY type ORDER BY type`)
	dump("attempts (step pid count distinct-keys)", `
		SELECT step, pid, count(*), count(DISTINCT key) FROM chaos_attempt GROUP BY step, pid ORDER BY step, pid`)
	dump("effects (step count)", `SELECT step, count(*) FROM chaos_effect GROUP BY step ORDER BY step`)
}

// ---- F, G: the workflow lease ----

// fenceGate, when set in the parent, holds the parent's run of fenced's
// workflow code between its steps until closed, and fenceHolding reports
// that it got there (test F: the takeover holds the workflow lease, in
// workflow code, while the thawed child writes). Holding it in workflow
// code does not depend on an eager launch, which is best effort.
var (
	fenceGate    atomic.Pointer[chan struct{}]
	fenceHolding atomic.Bool
)

func defineFenceStep(name string) *flows.Activity[int, int, struct{}] {
	return flows.DefineActivity(name, func(ctx context.Context, k flows.Key, v int) (int, struct{}, error) {
		if err := effect(ctx, name, k); err != nil {
			return 0, struct{}{}, err
		}
		return v + 1, struct{}{}, nil
	}, nil, flows.WithRetry(flows.RetryPolicy{Attempts: 3, Initial: 10 * time.Millisecond}))
}

var (
	fstep1 = defineFenceStep("chaos.fstep1")
	fstep2 = defineFenceStep("chaos.fstep2")

	// fenced holds its workflow code (in the child, in modes wf-pause and
	// wf-short) between its two steps until the test says go: the child
	// is frozen meanwhile. In wf-pause the code then goes on with an input
	// told apart from the takeover's (+1000): would its late completion be
	// stored, the output would show it, or the replay would diverge.
	fenced = flows.DefineWorkflow("chaos.fenced", 1, func(c flows.Ctx, in int) (int, error) {
		v, err := fstep1.Run(c, in)
		if err != nil {
			return 0, err
		}
		if (hangMode == "wf-pause" || hangMode == "wf-short") && !flows.Replaying(c) {
			if err := holdWorkflowCode("fence-0"); err != nil {
				return 0, err
			}
			if hangMode == "wf-pause" {
				v += 1000
			}
		} else if g := fenceGate.Load(); g != nil && !flows.Replaying(c) {
			fenceHolding.Store(true)
			<-*g
		}
		return fstep2.Run(c, v)
	})
)

// holdWorkflowCode marks key and blocks (in workflow code, under the
// workflow lease) until the test inserts key into chaos_go.
func holdWorkflowCode(key string) error {
	ctx := context.Background()
	pool := db.Load()
	if _, err := pool.Exec(ctx, `INSERT INTO chaos_marker (key) VALUES ($1) ON CONFLICT DO NOTHING`, key); err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM chaos_go WHERE key = $1`, key).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("never told to go")
}

func childFenced(ctx context.Context, eng *flows.Engine) {
	h, err := fenced.Start(ctx, eng, "fence-0", 1)
	if err != nil {
		fmt.Printf("RESULT: start: %v\n", err)
		return
	}
	wctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	out, err := h.Wait(wctx)
	fmt.Printf("RESULT: %d %v\n", out, err)
}

// F: a process freezes in workflow code (holding the workflow lease) for
// longer than its lease; another engine takes the workflow task over,
// replays it and holds the lease (its workflow code waits on a gate at the
// same point). The frozen process thaws and completes
// its task, with a different next step: the store's fence (owner and
// lease version of the execution) rejects it with ErrLeaseLost, nothing of
// it reaches the history, and the takeover's run is the one that ends.
//
// The takeover runs under another instance id (the owner differs), or
// under the frozen one's own id, "child" (a process restarted with a fixed
// id while the old one is frozen, not dead): then only the lease version
// tells the two apart.
func TestFrozenWorkflowTaskIsFencedOff(t *testing.T) {
	for _, owner := range []string{"takeover", "child"} {
		t.Run("owner="+owner, func(t *testing.T) { testFencedOff(t, owner) })
	}
}

// waitRefused waits for the thawed child's held task (its output from
// mark on) to be refused by the store for its lease; any other end of
// that task fails at once.
func waitRefused(t *testing.T, out *syncBuffer, mark int) {
	t.Helper()
	waitFor(t, "the thawed child's completion to be refused", 60*time.Second, func() bool {
		o := out.String()[mark:]
		if strings.Contains(o, "TASKEND fence-0 lost=false") || strings.Contains(o, "TASKOK fence-0") {
			t.Fatalf("the thawed child's task ended without a lease loss:\n%s", o)
		}
		return strings.Contains(o, "TASKEND fence-0 lost=true")
	})
}

func testFencedOff(t *testing.T, owner string) {
	t.Helper()
	pool := setup(t)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	fenceHolding.Store(false)
	fenceGate.Store(&release)
	t.Cleanup(func() { open(); fenceGate.Store(nil) })

	// 3s: a late renewal under load must not give the lease up before the
	// freeze.
	cmd, out := spawn(t, pool, "wf-pause", 1, envTTL+"=3s")
	waitFor(t, "child to hold its workflow code", 60*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker WHERE key = 'fence-0'`) == 1
	})
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = 'fence-0' AND owner_id = 'child' AND locked_until > now()`); n != 1 {
		t.Fatalf("workflow leases held by the child: %d", n)
	}
	var version int64
	if err := pool.QueryRow(context.Background(), `SELECT lease_version FROM flows.execution WHERE instance_id = 'fence-0'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	stats := &racerStats{name: owner + " (takeover)"}
	eng := startEngineWith(t, pool, flows.Options{
		InstanceID: owner, Observer: stats, Tracer: stats,
		StuckTimeout: 5 * time.Minute, // its workflow code waits on the gate
	})
	t.Cleanup(open) // before the engine's Stop, which waits for that code
	waitFor(t, "the takeover to hold the workflow lease, in its workflow code", 60*time.Second, func() bool {
		return fenceHolding.Load() &&
			count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = 'fence-0' AND owner_id = $1 AND locked_until > now() AND lease_version > $2`, owner, version) == 1
	})
	mark := len(out.String()) // the child's output from its thaw on
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO chaos_go (key) VALUES ('fence-0')`); err != nil {
		t.Fatal(err)
	}
	waitRefused(t, out, mark)
	// Evidence taken: the child must not claim the execution again (its
	// diverging code would replay the takeover's history).
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	// While the takeover still holds it (before its own fstep2), the refused
	// task left nothing.
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = 'fence-0' AND owner_id = $1 AND locked_until > now() AND lease_version > $2`, owner, version); n != 1 {
		t.Fatal("the takeover lost the workflow lease to the stale completion")
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1 AND name = 'chaos.fstep2'`, int(history.ActivityScheduled)); n != 0 {
		t.Fatalf("fstep2 scheduled %d times in the history before the takeover scheduled it", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.activity_task WHERE name = 'chaos.fstep2'`); n != 0 {
		t.Fatalf("fstep2 tasks before the takeover scheduled it: %d", n)
	}
	open()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	got, err := fenced.Handle(eng, "fence-0").Wait(ctx)
	if err != nil {
		t.Fatalf("execution: %v", err)
	}
	if got != 3 {
		t.Fatalf("output %d, want 3: the stale task's step (1000+) was recorded", got)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.fstep2' AND pid <> $1`, os.Getpid()); n != 0 {
		t.Fatalf("fstep2 ran %d times in the frozen child", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_effect`); n != 2 {
		t.Fatalf("effects = %d, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.ActivityScheduled)); n != 2 {
		t.Fatalf("activities scheduled = %d, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.ActivityCompleted)); n != 2 {
		t.Fatalf("activity outcomes = %d, want 2", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE status = 'completed'`); n != 1 {
		t.Fatalf("completed executions = %d, want 1", n)
	}
	if stats.taskOK.Load() == 0 {
		t.Fatalf("the takeover ran no workflow task: %v", stats)
	}
	t.Log(stats)
}

// G: a process freezes in workflow code for less than its lease; another
// engine polls meanwhile. Nothing is lost: across the freeze the frozen
// process keeps the workflow lease (same owner, same lease version, never
// taken over), its heartbeat renews it after the thaw, and its held task
// completes without a lease loss, the execution ending with the right
// output and each step run once.
//
// The freeze is aligned on the heartbeat so that it always spans a renewal
// tick: the test waits for a renewal to land (locked_until moves), then half
// an interval, then freezes for 0.8 interval (interval = TTL/3 = 5s), so the
// next tick comes due while frozen and runs late, on thaw.
//
// Limits: on thaw the overdue tick renews first (the renewal checks owner
// and lease version, not locked_until) and only then gives up leases not
// renewed for ttl - interval (heartbeat.expire). A freeze within the TTL
// therefore loses the lease locally only when it lands while a renewal is
// in flight and lasts ttl - interval or more (that renewal's send time is
// then too old), or when the renewal on thaw fails; another engine can take
// the lease over only once the full TTL has passed. A loss is safe anyway:
// the work is fenced and runs again.
//
// The other engine may still run the second step, or the last task: an
// eager launch is best effort (it needs a free activity slot, and the
// child's activity poll, due at once on thaw, holds the free slots while
// it claims), and a step not launched eagerly is queued for any engine.
func TestShortFreezeKeepsTheLease(t *testing.T) {
	const ttl, interval = 15 * time.Second, 5 * time.Second
	pool := setup(t)
	cmd, out := spawn(t, pool, "wf-short", 1, envTTL+"="+ttl.String())
	waitFor(t, "child to hold its workflow code", 60*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker WHERE key = 'fence-0'`) == 1
	})
	stats := &racerStats{name: "thief"}
	startEngineWith(t, pool, flows.Options{InstanceID: "thief", Observer: stats, Tracer: stats})
	lease := func() (version int64, until time.Time) {
		t.Helper()
		if err := pool.QueryRow(context.Background(), `
			SELECT lease_version, locked_until FROM flows.execution
			WHERE instance_id = 'fence-0' AND owner_id = 'child' AND locked_until > now()`).Scan(&version, &until); err != nil {
			t.Fatalf("the child does not hold the workflow lease: %v", err)
		}
		return version, until
	}
	version, first := lease()
	var renewed time.Time
	waitFor(t, "a renewal of the child's workflow lease", 3*interval, func() bool {
		_, renewed = lease()
		return renewed.After(first)
	})
	time.Sleep(interval / 2)
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(interval * 4 / 5) // spans the next renewal tick, due interval after the last
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	// The tick missed during the freeze runs on thaw, and renews: same
	// owner, same lease version.
	waitFor(t, "the thawed child to renew its lease", 2*interval, func() bool {
		v, until := lease()
		if v != version {
			t.Fatalf("lease version %d, was %d: the lease was taken during the freeze", v, version)
		}
		return until.After(renewed)
	})
	// Still the child's lease, the one it had: nobody took it meanwhile.
	if n := count(t, pool, `
		SELECT count(*) FROM flows.execution
		WHERE instance_id = 'fence-0' AND owner_id = 'child' AND lease_version = $1 AND locked_until > now()`, version); n != 1 {
		t.Fatal("the child lost its workflow lease during a freeze shorter than the lease")
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO chaos_go (key) VALUES ('fence-0')`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the child to end its execution", 2*time.Minute, func() bool {
		return strings.Contains(out.String(), "RESULT:")
	})
	t.Log(stats)
	if !strings.Contains(out.String(), "RESULT: 3 <nil>") {
		t.Fatalf("child: %s", out.String())
	}
	// Its held task completed (the start, then the held one, at least).
	if strings.Contains(out.String(), "TASKEND") || strings.Contains(out.String(), "ACTEND") ||
		strings.Count(out.String(), "TASKOK fence-0") < 2 {
		t.Fatalf("the child lost a lease or failed a task:\n%s", out.String())
	}
	if stats.taskLost.Load() != 0 || stats.attemptsLost.Load() != 0 {
		t.Fatalf("the other engine lost a lease: %v", stats)
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE status = 'completed'`); n != 1 {
		t.Fatalf("completed executions = %d, want 1", n)
	}
	for _, step := range []string{"chaos.fstep1", "chaos.fstep2"} {
		if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = $1`, step); n != 1 {
			t.Fatalf("%s ran %d times, want 1", step, n)
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.fstep1' AND pid = $1`, cmd.Process.Pid); n != 1 {
		t.Fatal("fstep1 did not run in the child")
	}
	if n := count(t, pool, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.ActivityScheduled)); n != 2 {
		t.Fatalf("activities scheduled = %d, want 2", n)
	}
	_ = cmd.Wait()
}

// ---- H: children, continue-as-new, timers, compensation ----

const (
	trees      = 6
	treeRounds = 3
	treeTimer  = 300 * time.Millisecond
	leafTimer  = 100 * time.Millisecond
)

type treeIn struct{ ID, Round, Acc int }

// slowEffect is effect, after a short delay: the work is spread over time,
// so that the freeze and the kill land in its middle.
func slowEffect(ctx context.Context, name string, k flows.Key) error {
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return effect(ctx, name, k)
}

func defineUndoable(name string) *flows.Activity[int, int, string] {
	return flows.DefineActivity(name, func(ctx context.Context, k flows.Key, v int) (int, string, error) {
		return v + 1, name + "-of-" + string(k), slowEffect(ctx, name, k)
	}, func(ctx context.Context, k flows.Key, data string) error {
		if data != name+"-of-"+string(k) {
			return flows.Permanent(fmt.Errorf("compensation data %q lost", data))
		}
		return slowEffect(ctx, "undo-"+name, flows.Key(string(k)+"/undo"))
	}, flows.WithRetry(flows.RetryPolicy{Attempts: 5, Initial: 10 * time.Millisecond}))
}

// timed sleeps d on a durable timer and checks, by the history's clock,
// that it did not fire early.
func timed(c flows.Ctx, d time.Duration) error {
	t0 := flows.Now(c)
	if err := flows.Sleep(c, d); err != nil {
		return err
	}
	if el := flows.Now(c).Sub(t0); el < d {
		return flows.Permanent(fmt.Errorf("timer of %v fired after %v", d, el))
	}
	return nil
}

var (
	book     = defineUndoable("chaos.book")
	leafStep = defineUndoable("chaos.leafstep")
	decline  = flows.DefineActivity("chaos.decline", func(ctx context.Context, k flows.Key, _ int) (int, struct{}, error) {
		if _, err := db.Load().Exec(ctx, `INSERT INTO chaos_attempt (key, step, pid) VALUES ($1, $2, $3)`,
			string(k), "chaos.decline", os.Getpid()); err != nil {
			return 0, struct{}{}, err
		}
		return 0, struct{}{}, flows.Permanent(errDeclined)
	}, nil)

	leaf = flows.DefineWorkflow("chaos.leaf", 1, func(c flows.Ctx, v int) (int, error) {
		x, err := leafStep.Run(c, v)
		if err != nil {
			return 0, err
		}
		if err := timed(c, leafTimer); err != nil {
			return 0, err
		}
		return 2 * x, nil
	})

	// tree books, sleeps, runs two children, and continues as new for
	// treeRounds rounds; the last round of an odd tree is declined: it
	// compensates its booking and its two children.
	tree = flows.DefineWorkflow("chaos.tree", 1, func(c flows.Ctx, in treeIn) (int, error) {
		v, err := book.Run(c, in.ID*100+in.Round)
		if err != nil {
			return 0, err
		}
		if err := timed(c, treeTimer); err != nil {
			return 0, err
		}
		prefix := treeID(in.ID) + "/r" + strconv.Itoa(in.Round)
		f0 := leaf.Child(c, prefix+"/leaf-0", v)
		f1 := leaf.Child(c, prefix+"/leaf-1", v+1)
		a, err := f0.Get(c)
		if err != nil {
			return 0, err
		}
		b, err := f1.Get(c)
		if err != nil {
			return 0, err
		}
		acc := in.Acc + a + b
		if in.Round < treeRounds-1 {
			return 0, flows.ContinueAsNew(c, treeIn{ID: in.ID, Round: in.Round + 1, Acc: acc})
		}
		if in.ID%2 == 1 {
			_, err := decline.Run(c, acc)
			return 0, err
		}
		return acc, nil
	})
)

func treeID(id int) string { return "tree-" + strconv.Itoa(id) }

// treeWant is the output of an even tree.
func treeWant(id int) int {
	acc := 0
	for r := range treeRounds {
		v := id*100 + r + 1
		acc += 2*(v+1) + 2*(v+2)
	}
	return acc
}

func childTrees(ctx context.Context, eng *flows.Engine) {
	for i := range trees {
		if _, err := tree.Start(ctx, eng, treeID(i), treeIn{ID: i}); err != nil {
			fmt.Printf("RESULT: start: %v\n", err)
			return
		}
	}
	select {} // frozen, thawed, then killed
}

// H: a process runs trees of workflows (children, durable timers,
// continue-as-new, a compensated end) and is frozen past its leases while
// two engines take over, thawed (its stale writes race theirs), then
// killed. Every execution of the chains ends once with the right result,
// every activity takes effect once per key (keys differ per execution), no
// history records anything twice, and exactly the last round of the odd
// trees is compensated, children included.
func TestTreesSurviveFreezeAndKill(t *testing.T) {
	pool := setup(t)
	cmd, out := spawn(t, pool, "tree", trees)
	effects := func() int { return count(t, pool, `SELECT count(*) FROM chaos_effect`) }
	ended := func() int {
		return count(t, pool, `SELECT count(*) FROM flows.execution WHERE workflow = 'chaos.tree' AND status IN ('completed', 'compensated', 'failed')`)
	}
	waitFor(t, "the child to make progress", 2*time.Minute, func() bool { return effects() >= 2*trees })
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if n := ended(); n == trees {
		t.Fatal("every tree ended before the freeze: nothing was interrupted")
	}
	t.Logf("frozen after %d effects", effects())

	stats := []*racerStats{{name: "rec-0"}, {name: "rec-1"}}
	engines := make([]*flows.Engine, len(stats))
	for i, s := range stats {
		engines[i] = startEngineWith(t, pool, flows.Options{InstanceID: s.name, Observer: s, Tracer: s})
	}
	byUs := func() int {
		return count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE pid = $1`, os.Getpid())
	}
	waitFor(t, "the engines to take over", 2*time.Minute, func() bool { return byUs() >= 3 || ended() == trees })
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	byChild := func() int {
		return count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE pid = $1`, cmd.Process.Pid)
	}
	// The thawed child takes part in the race: it makes attempts (new
	// work it claims), or writes what it held when frozen and is refused
	// (lost=true), unless nothing is left by then.
	before, mark := byChild(), len(out.String())
	refused := func() int { return strings.Count(out.String()[mark:], "lost=true") }
	waitFor(t, "the thawed child to race the engines", time.Minute, func() bool {
		return byChild() > before || refused() > 0 || ended() == trees
	})
	t.Logf("after the thaw the child made %d attempts and had %d writes refused (trees ended: %d)",
		byChild()-before, refused(), ended())
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for i := range trees {
		got, err := tree.Handle(engines[i%2], treeID(i)).Wait(ctx)
		if i%2 == 0 {
			if err != nil || got != treeWant(i) {
				t.Fatalf("%s: %d, %v; want %d", treeID(i), got, err, treeWant(i))
			}
			continue
		}
		var re *flows.RunError
		if !errors.As(err, &re) || re.Status != flows.StatusCompensated || !strings.Contains(err.Error(), "declined") {
			t.Fatalf("%s: %v, want compensated after declined", treeID(i), err)
		}
	}
	for _, s := range stats {
		t.Log(s)
	}
	// The child's stale writes were all refused by the lease fence: none
	// got past it to a later check (a sequence or schedule conflict), none
	// parked an execution.
	// (Parks would show in the executions' statuses, checked below.)
	if strings.Contains(out.String(), "invalid=true") {
		t.Fatal("a completion of the child was refused past the lease fence: a stale write got past it")
	}
	assertTrees(t, pool, cmd.Process.Pid)
}

func assertTrees(t *testing.T, pool *pgxpool.Pool, childPid int) {
	t.Helper()
	finals := assertTreeExecutions(t, pool)
	assertTreeHistory(t, pool)
	assertTreeEffects(t, pool, finals)
	if n := count(t, pool, `SELECT count(*) FROM flows.activity_task`); n != 0 {
		t.Fatalf("activity tasks left: %d", n)
	}
	// Both sides did work: the child before its kill, the engines after.
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE pid = $1`, childPid); n == 0 {
		t.Fatal("the child made no attempt")
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE pid = $1`, os.Getpid()); n == 0 {
		t.Fatal("the recovering engines made no attempt")
	}
}

// assertTreeExecutions checks the executions of the chains and of their
// children, and returns the last execution id of each tree.
func assertTreeExecutions(t *testing.T, pool *pgxpool.Pool) map[int]int64 {
	t.Helper()
	// Each chain: one execution per round, ended once.
	finals := make(map[int]int64)
	for i := range trees {
		rows, err := pool.Query(context.Background(), `SELECT id, status FROM flows.execution WHERE instance_id = $1 ORDER BY id`, treeID(i))
		if err != nil {
			t.Fatal(err)
		}
		var statuses []string
		var last int64
		for rows.Next() {
			var s string
			if err := rows.Scan(&last, &s); err != nil {
				t.Fatal(err)
			}
			statuses = append(statuses, s)
		}
		rows.Close()
		want := strings.Repeat("continued,", treeRounds-1) + "completed"
		if i%2 == 1 {
			want = strings.Repeat("continued,", treeRounds-1) + "compensated"
		}
		if got := strings.Join(statuses, ","); got != want {
			t.Fatalf("%s executions: %s, want %s", treeID(i), got, want)
		}
		finals[i] = last
	}
	// Each child: one execution; the last round's of odd trees reopened
	// and compensated.
	for i := range trees {
		for r := range treeRounds {
			for j := range 2 {
				id := treeID(i) + "/r" + strconv.Itoa(r) + "/leaf-" + strconv.Itoa(j)
				want := "completed"
				if i%2 == 1 && r == treeRounds-1 {
					want = "compensated"
				}
				if n := count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = $1 AND status = $2`, id, want); n != 1 {
					t.Fatalf("%s: %d executions %s (of %d), want 1", id, n, want,
						count(t, pool, `SELECT count(*) FROM flows.execution WHERE instance_id = $1`, id))
				}
			}
		}
	}
	execs := trees*treeRounds + 2*trees*treeRounds
	if n := count(t, pool, `SELECT count(*) FROM flows.execution`); n != execs {
		t.Fatalf("executions = %d, want %d", n, execs)
	}
	return finals
}

// assertTreeHistory checks that the histories record everything once.
func assertTreeHistory(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	odd := trees / 2
	for _, h := range []struct {
		typ  history.Type
		name string
		want int
	}{
		{history.TimerScheduled, "", trees*treeRounds + 2*trees*treeRounds},
		{history.TimerFired, "", trees*treeRounds + 2*trees*treeRounds},
		{history.ChildScheduled, "", 2 * trees * treeRounds},
		{history.ChildCompleted, "", 2 * trees * treeRounds},
		{history.ExecutionContinuedAsNew, "", trees * (treeRounds - 1)},
		{history.ActivityScheduled, "chaos.book", trees * treeRounds},
		{history.ActivityScheduled, "chaos.leafstep", 2 * trees * treeRounds},
		{history.ActivityScheduled, "chaos.decline", odd},
		{history.ActivityCompleted, "", 3 * trees * treeRounds},
		{history.ActivityFailed, "", odd},
		{history.CompensationFailed, "", 0},
	} {
		q, args := `SELECT count(*) FROM flows.history WHERE type = $1`, []any{int(h.typ)}
		if h.name != "" {
			q, args = q+` AND name = $2`, append(args, h.name)
		}
		if n := count(t, pool, q, args...); n != h.want {
			t.Fatalf("history events of type %d %s: %d, want %d", h.typ, h.name, n, h.want)
		}
	}
}

// assertTreeEffects checks that every activity and compensator took effect
// once per key, and that the undone bookings are the right ones.
func assertTreeEffects(t *testing.T, pool *pgxpool.Pool, finals map[int]int64) {
	t.Helper()
	odd := trees / 2
	// Effects: once per key; every key that ran took effect; at least once.
	for _, e := range []struct {
		step string
		want int
	}{
		{"chaos.book", trees * treeRounds},
		{"chaos.leafstep", 2 * trees * treeRounds},
		{"undo-chaos.book", odd},
		{"undo-chaos.leafstep", 2 * odd},
	} {
		if n := count(t, pool, `SELECT count(*) FROM chaos_effect WHERE step = $1`, e.step); n != e.want {
			t.Fatalf("%s effects = %d, want %d", e.step, n, e.want)
		}
		if n := count(t, pool, `SELECT count(DISTINCT key) FROM chaos_attempt WHERE step = $1`, e.step); n != e.want {
			t.Fatalf("%s attempted with %d keys, want %d", e.step, n, e.want)
		}
	}
	if n := count(t, pool, `SELECT count(DISTINCT key) FROM chaos_attempt WHERE step = 'chaos.decline'`); n != odd {
		t.Fatalf("decline attempted with %d keys, want %d", n, odd)
	}
	// The undone bookings are the last executions' of the odd trees.
	rows, err := pool.Query(context.Background(), `SELECT key FROM chaos_effect WHERE step = 'undo-chaos.book'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(strings.TrimSuffix(k, "/undo"), ":")
		id, _ := strconv.Atoi(strings.TrimPrefix(parts[0], "tree-"))
		exec, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if id%2 != 1 || exec != finals[id] {
			t.Fatalf("undone booking %s: not the last execution (%d) of an odd tree", k, finals[id])
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
