package worker_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// syncBuffer is a bytes.Buffer safe for concurrent writes (a log sink).
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// T9.2: a child started in the task that continues the workflow as new runs
// on, detached (as the children started before do); one started in a task
// that ends it otherwise is not started (running children are asked to
// cancel at the end). Activities started in a last task are not started
// either. Drops are logged, not silent; an end without any is not.
func TestChildrenStartedAtTheEnd(t *testing.T) {
	st := mem.New(nil)
	reg := newRegistry()
	reg.addActivity("double", &worker.Activity{Run: func(_ context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
		return t.Input, nil, nil
	}})
	reg.addWorkflow("kid", func(ctx coro.Context, _ []byte) ([]byte, error) {
		v, _ := executor.FromContext(ctx).SignalChannel("go").Receive(ctx) // outlives its parent's chain
		return v, nil
	})
	reg.addWorkflow("parent", func(ctx coro.Context, in []byte) ([]byte, error) {
		x := executor.FromContext(ctx)
		switch string(in) {
		case "continue":
			x.StartChild(ctx, "kid-continued", "kid", 1, q, nil) // not awaited
			x.ScheduleActivity(ctx, "double", q, []byte("1"))    // not awaited: not started
			return nil, &executor.ContinueAsNewError{Input: []byte("then")}
		case "end":
			x.StartChild(ctx, "kid-dropped", "kid", 1, q, nil) // not awaited
			return []byte("ended"), nil
		case "plain":
			return []byte("plain"), nil
		}
		return []byte("continued"), nil
	})
	var logs syncBuffer
	startWorker(t, st, reg, worker.Options{Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	create(t, st, "p0", "parent", "plain")
	create(t, st, "p1", "parent", "continue")
	create(t, st, "p2", "parent", "end")
	if e := waitEnd(t, st, "p1"); e.Status != store.StatusCompleted || string(e.Output) != "continued" {
		t.Fatalf("p1: %s %q", e.Status, e.Output)
	}
	// Detached: the chain ended, the child runs on until its signal.
	if e, err := st.GetExecution(context.Background(), "kid-continued"); err != nil || e.Status != store.StatusRunning {
		t.Fatalf("the child started with the continue-as-new: %+v %v", e, err)
	}
	if err := st.Signal(context.Background(), "kid-continued", "go", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if e := waitEnd(t, st, "kid-continued"); e.Status != store.StatusCompleted || string(e.Output) != "ok" {
		t.Fatalf("kid-continued: %s %q", e.Status, e.Output)
	}
	if e := waitEnd(t, st, "p2"); e.Status != store.StatusCompleted {
		t.Fatalf("p2: %s", e.Status)
	}
	if _, err := st.GetExecution(context.Background(), "kid-dropped"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the child started with the end exists: %v", err)
	}
	waitEnd(t, st, "p0")
	want := []string{
		"instance=p1 status=continued activities=1 children=0", // the activity, not the child
		"instance=p2 status=completed activities=0 children=1",
	}
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), want[0]) || !strings.Contains(logs.String(), want[1]) {
		if time.Now().After(deadline) {
			t.Fatalf("drops not logged as %q:\n%s", want, logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if strings.Contains(logs.String(), "instance=p0") {
		t.Fatalf("an end without drops logged one:\n%s", logs.String())
	}
}
