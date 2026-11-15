package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// The end hook sees an end with the execution's identity; an error or a
// panic rejects the completion, which can then be retried under the same
// lease.
func testEndHook(t *testing.T, h Harness) {
	if h.SetHook == nil {
		t.Skip("no hook support")
	}
	s := h.New(t)
	var (
		mu   sync.Mutex
		seen []store.EndInfo
		fail error
		boom bool
	)
	h.SetHook(s, func(_ context.Context, e store.EndInfo) error {
		mu.Lock()
		defer mu.Unlock()
		if boom {
			panic("hook bug")
		}
		if fail != nil {
			return fail
		}
		seen = append(seen, e)
		return nil
	})
	set := func(err error, p bool) {
		mu.Lock()
		fail, boom = err, p
		mu.Unlock()
	}
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	end := &store.Completion{
		History:  []history.Event{task.Pending[0].Event, {Seq: 2, Type: history.ExecutionFailed, Err: "x"}},
		Consumed: pendingIDs(task), Status: store.StatusFailed, Err: "x", ErrKinds: "k",
	}
	end.History[0].Seq = 1
	set(errors.New("outbox down"), false)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, end); err == nil {
		t.Fatal("completion committed despite the hook's error")
	}
	set(nil, true)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, end); err == nil {
		t.Fatal("completion committed despite the hook's panic")
	}
	if e, _ := s.GetExecution(ctx, "i1"); e.Status != store.StatusRunning {
		t.Fatalf("status after a rejected end: %s", e.Status)
	}
	set(nil, false)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, end); err != nil {
		t.Fatalf("retry under the same lease: %v", err)
	}
	mu.Lock()
	if len(seen) != 1 {
		t.Fatalf("hook calls %+v", seen)
	}
	if e := seen[0]; e.InstanceID != "i1" || e.Workflow != "wf" || e.Version != 1 || e.Status != store.StatusFailed ||
		e.Err != "x" || e.ErrKinds != "k" || e.ExecutionID == 0 {
		t.Fatalf("end %+v", e)
	}
	mu.Unlock()
}

// The end hook sees parks and terminations; continue-as-new and running
// completions do not call it.
func testEndHookCalls(t *testing.T, h Harness) {
	if h.SetHook == nil {
		t.Skip("no hook support")
	}
	s := h.New(t)
	var (
		mu   sync.Mutex
		seen []store.EndInfo
	)
	h.SetHook(s, func(_ context.Context, e store.EndInfo) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, e)
		return nil
	})
	mustCreate(t, s, "i2")
	task := claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{Status: store.StatusNeedsAttention, Err: "nd"}); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, "i3")
	task = claimOne(t, s, owner, h.TTL)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionContinuedAsNew}},
		Consumed: pendingIDs(task), Status: store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Terminate(ctx, "i3", "stop"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("hook calls %+v", seen)
	}
	if e := seen[0]; e.InstanceID != "i2" || e.Status != store.StatusNeedsAttention || e.Err != "nd" {
		t.Fatalf("park %+v", e)
	}
	if e := seen[1]; e.InstanceID != "i3" || e.Status != store.StatusTerminated || e.Err != "stop" {
		t.Fatalf("termination %+v", e)
	}
}
