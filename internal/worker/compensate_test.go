package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// compRegistry adds compensators to registry.
type compRegistry struct {
	*registry
	comps map[string]*worker.Activity
}

func (r compRegistry) Activity(name string, compensate bool) (*worker.Activity, bool) {
	if compensate {
		a, ok := r.comps[name]
		return a, ok
	}
	return r.registry.Activity(name, false)
}

func startOn(t *testing.T, st store.Store, owner string, wfs worker.Workflows, acts worker.Activities, queues []string, eager bool) {
	t.Helper()
	w, err := worker.New(st, wfs, acts, worker.Options{
		Owner: owner, Queues: queues, LeaseTTL: 5 * time.Second, PollMax: 20 * time.Millisecond,
		HandBack: 20 * time.Millisecond, Eager: eager,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Stop(ctx)
	})
}

func waitStatus(t *testing.T, st store.Store, id string, want store.Status) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		e, err := st.GetExecution(context.Background(), id)
		if err == nil && e.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: status %s (%v), want %s", id, e.Status, err, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Review of P5 (H1): in a mixed deploy, whether an activity is compensated
// depends on the binary that ran it (it recorded compensation data), not on
// the one planning the compensation (here a binary that knows the activity
// without its compensator).
func TestCompensationInMixedDeploy(t *testing.T) {
	st := mem.New(nil)
	var undone atomic.Int32
	x := &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		return []byte(`"ok"`), []byte(`"undo-data"`), nil
	}}
	xNoComp := &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		return []byte(`"ok"`), nil, nil
	}}
	comp := &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		if string(t.Input) != `"undo-data"` {
			return nil, nil, errors.New("wrong data " + string(t.Input))
		}
		undone.Add(1)
		return nil, nil, nil
	}}
	// Old binary: runs the workflow, knows x without a compensator.
	old := newRegistry()
	old.addWorkflow("saga", func(ctx coro.Context, _ []byte) ([]byte, error) {
		if _, err := executor.FromContext(ctx).ScheduleActivity(ctx, "x", "qa", nil).Get(ctx); err != nil {
			return nil, err
		}
		return nil, errors.New("declined")
	})
	old.addActivity("x", xNoComp)
	// The old binary plans the compensation but runs no activity of qa.
	startOn(t, st, "old", old, compRegistry{old, nil}, []string{q}, false)
	// New binary: knows x with its compensator, runs x and the compensation.
	nw := newRegistry()
	nw.addActivity("x", x)
	startOn(t, st, "new", nw, compRegistry{nw, map[string]*worker.Activity{"x": comp}}, []string{"qa"}, false)
	if _, _, err := st.CreateExecution(context.Background(), store.NewExecution{InstanceID: "mixed", Workflow: "saga", Version: 1, Queue: q}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, "mixed", store.StatusCompensated)
	if n := undone.Load(); n != 1 {
		t.Fatalf("compensator ran %d times", n)
	}
}

// Review of T10.7: the binary running the final attempt decides whether the
// activity is compensated. Data kept from an earlier uncertain attempt (run
// by a binary with the compensator) is not recorded when the final attempt
// runs where the activity has no compensator: no compensation that no
// binary can run.
func TestKeptCompDataNeedsACompensator(t *testing.T) {
	st := mem.New(nil)
	wfs := newRegistry()
	wfs.addWorkflow("saga", func(ctx coro.Context, _ []byte) ([]byte, error) {
		_, err := executor.FromContext(ctx).ScheduleActivity(ctx, "x", "qa", nil).Get(ctx)
		if err == nil {
			err = errors.New("declined")
		}
		return nil, err
	})
	startOn(t, st, "wf", wfs, compRegistry{wfs, nil}, []string{q}, false)

	// Binary A, with the compensator: its attempt 1 may have had its effect.
	var ran atomic.Int32
	withComp := newRegistry()
	withComp.addActivity("x", &worker.Activity{
		Retry: worker.RetryPolicy{Attempts: 2, Initial: 3 * time.Second},
		Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
			ran.Add(1)
			return nil, []byte(`"charge-1"`), worker.ErrUncertain
		},
	})
	comp := &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) { return nil, nil, nil }}
	a, err := worker.New(st, withComp, compRegistry{withComp, map[string]*worker.Activity{"x": comp}}, worker.Options{
		Owner: "a", Queues: []string{"qa"}, LeaseTTL: 5 * time.Second, PollMax: 20 * time.Millisecond, HandBack: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.CreateExecution(context.Background(), store.NewExecution{InstanceID: "kept-nocomp", Workflow: "saga", Version: 1, Queue: q}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for ran.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("attempt 1 did not run")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = a.Stop(ctx) // before its retry is due
	cancel()
	if ran.Load() != 1 {
		t.Skip("binary A ran the retry too: machine too slow for this test")
	}

	// Binary B knows x without a compensator: its final attempt fails.
	noComp := newRegistry()
	noComp.addActivity("x", &worker.Activity{Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		return nil, nil, errors.New("declined")
	}})
	startOn(t, st, "b", noComp, compRegistry{noComp, nil}, []string{"qa"}, false)
	waitStatus(t, st, "kept-nocomp", store.StatusFailed)
}
