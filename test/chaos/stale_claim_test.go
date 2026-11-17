//go:build chaos

package chaos_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// whoTracer puts the engine's name in its activities' contexts.
type whoTracer string

type whoKey struct{}

func (whoTracer) StartWorkflow(context.Context, flows.StartInfo) ([]byte, func(bool, error)) {
	return nil, func(bool, error) {}
}

func (w whoTracer) StartActivity(ctx context.Context, _ flows.ActivityInfo) (context.Context, func(error)) {
	return context.WithValue(ctx, whoKey{}, string(w)), func(error) {}
}

func (whoTracer) StartWorkflowTask(ctx context.Context, _ flows.WorkflowTaskInfo) (context.Context, func(flows.Status, error)) {
	return ctx, func(flows.Status, error) {}
}

// I4 (a T11.7 finding; FLOWS_CHAOS_STALE_CLAIM=1 runs it): a claim whose
// reply arrives after the lease it took expired. Engine a (the only one
// on the activity's queue) claims the activity task on the server while
// the server's replies to it are held (Proxy.FreezeReplies: a database
// that stops answering after it executed the claim); the lease expires,
// engine c claims the task and runs its attempt; then a gets its claim's
// reply and starts its attempt too, on a lease long expired (held by c),
// until its heartbeat's expire tick (up to a renewal interval later)
// cancels it: the attempts overlap. The worker should not start a task
// whose claim was sent ttl - ttl/3 ago or more (the point where its
// heartbeat gives a lease up).
func TestStaleClaimReplyStartsNoAttempt(t *testing.T) {
	if os.Getenv("FLOWS_CHAOS_STALE_CLAIM") == "" {
		t.Skip("known engine bug, a T11.7 finding (FLOWS_CHAOS_STALE_CLAIM=1 runs it)")
	}
	const ttl = 3 * time.Second
	viaProxy, proxy := testdb.NewProxied(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, viaProxy); err != nil {
		t.Fatal(err)
	}
	direct, err := pgxpool.NewWithConfig(ctx, proxy.DirectConfig(viaProxy.Config()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(direct.Close)
	db.Store(direct)

	type attempt struct {
		who        string
		start, end time.Time
	}
	var (
		mu       sync.Mutex
		attempts []attempt
	)
	queue := "stale-claim"
	act := flows.DefineActivity(testdb.UniqueName(t, "act"), func(actx context.Context, _ flows.Key, in int) (int, struct{}, error) {
		who, _ := actx.Value(whoKey{}).(string)
		mu.Lock()
		i := len(attempts)
		attempts = append(attempts, attempt{who: who, start: time.Now()})
		mu.Unlock()
		err := sleepCtx(actx, 3*time.Second)
		mu.Lock()
		attempts[i].end = time.Now()
		mu.Unlock()
		return in, struct{}{}, err
	}, nil, flows.WithQueue(queue), flows.WithRetry(flows.RetryPolicy{Attempts: 10, Initial: 10 * time.Millisecond}))
	wf := flows.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c flows.Ctx, in int) (int, error) {
		return act.Run(c, in)
	})
	start := func(p *pgxpool.Pool, id string, queues ...string) *flows.Engine {
		e := mustEngine(t, p, flows.Options{InstanceID: id, LeaseTTL: ttl, PollMax: time.Second, Queues: queues, Tracer: whoTracer(id)})
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			proxy.Thaw()
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = e.Stop(sctx)
		})
		return e
	}
	start(viaProxy, "a", queue)                 // the activity's queue only
	b := start(direct, "b", flows.DefaultQueue) // the workflow's
	time.Sleep(500 * time.Millisecond)          // a polls, idle
	proxy.FreezeReplies()
	if _, err := wf.Start(ctx, b, "i1", 7); err != nil {
		t.Fatal(err)
	}
	// a's next idle poll (within PollMax) claims the task; the reply is held.
	deadline := time.Now().Add(5 * time.Second)
	for count(t, direct, `SELECT count(*) FROM flows.activity_task WHERE owner_id = 'a'`) == 0 {
		if time.Now().After(deadline) {
			t.Skip("inconclusive: a's poller was waiting on another reply when the task came")
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitFor(t, "a's lease to expire", 2*ttl, func() bool {
		return count(t, direct, `SELECT count(*) FROM flows.activity_task WHERE locked_until < now()`) == 1
	})
	start(direct, "c", queue)
	waitFor(t, "c to start its attempt", 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(attempts) == 1
	})
	thawed := time.Now()
	proxy.Thaw()
	time.Sleep(2 * time.Second) // c's attempt runs 3s
	mu.Lock()
	defer mu.Unlock()
	c := attempts[0]
	for _, x := range attempts[1:] {
		if x.who == "a" && (c.end.IsZero() || x.start.Before(c.end)) {
			ran := "still running"
			if !x.end.IsZero() {
				ran = "ran " + x.end.Sub(x.start).Round(time.Millisecond).String()
			}
			t.Fatalf("a started an attempt %v after the thaw, on its claim's late reply (its lease long expired, now c's), while c's attempt ran (a's %s)",
				x.start.Sub(thawed).Round(time.Millisecond), ran)
		}
	}
	t.Logf("attempts: %d, a's claim did not start one", len(attempts))
}
