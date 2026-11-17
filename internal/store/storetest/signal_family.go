package storetest

import (
	"slices"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// claimID claims the ready tasks and returns the one of instance id; the
// others are released.
func claimID(t *testing.T, s store.Store, h Harness, id string) store.WorkflowTask {
	t.Helper()
	var got *store.WorkflowTask
	for _, x := range claim(t, s, owner, h.TTL) {
		if x.Execution.InstanceID == id {
			got = &x
			continue
		}
		if err := s.ReleaseWorkflowTask(ctx, x.Lease, 0); err != nil {
			t.Fatal(err)
		}
	}
	if got == nil {
		t.Fatalf("%s not claimed", id)
	}
	return *got
}

// completeWith completes task: its pending events, then evs, recorded
// (sequence numbers from seq), with c's other fields.
func (seq lockSeq) completeWith(t *testing.T, s store.Store, task store.WorkflowTask, c *store.Completion, evs ...history.Event) {
	t.Helper()
	id := task.Execution.InstanceID
	c.Consumed = pendingIDs(task)
	for _, ev := range append(pendingEvents(task), evs...) {
		seq[id]++
		ev.Seq = seq[id]
		c.History = append(c.History, ev)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatalf("complete %s: %v", id, err)
	}
}

func pendingEvents(task store.WorkflowTask) []history.Event {
	evs := make([]history.Event, len(task.Pending))
	for i, p := range task.Pending {
		evs[i] = p.Event
	}
	return evs
}

// family creates parent p and starts its child c (schedule id 1) in p's
// first task; with signal, p signals c in the same task (schedule id 2).
// Both are left released, c's start event pending.
func family(t *testing.T, s store.Store, h Harness, seq lockSeq, signal bool) {
	t.Helper()
	mustCreate(t, s, "p")
	task := claimID(t, s, h, "p")
	c := &store.Completion{Children: []store.NewExecution{{InstanceID: "c", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}}
	evs := []history.Event{{Type: history.ChildScheduled, ScheduleID: 1, Name: "cw", Version: 1, Ref: "c", Queue: queue}}
	if signal {
		r := store.SignalRequest{ScheduleID: 2, InstanceID: "c", Name: "hi", Payload: []byte("1")}
		c.Signals = []store.SignalRequest{r}
		evs = append(evs, signalSent(0, r))
	}
	seq.completeWith(t, s, task, c, evs...)
}

// T12.1: a parent starts a child and signals it in the same checkpoint: the
// child exists by the time the signal is delivered (its first task has
// both), and the parent's answer is SignalDelivered.
func testSignalWorkflowNewChild(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	family(t, s, h, seq, true)
	kid := claimID(t, s, h, "c")
	if got := pendingTypes(kid); len(got) != 2 || got[0] != history.ExecutionStarted || got[1] != history.SignalReceived ||
		string(kid.Pending[1].Event.Payload) != "1" {
		t.Fatalf("child got %v", got)
	}
	if err := s.ReleaseWorkflowTask(ctx, kid.Lease, 0); err != nil {
		t.Fatal(err)
	}
	p := claimID(t, s, h, "p")
	if len(p.Pending) != 1 || p.Pending[0].Event.Type != history.SignalDelivered || p.Pending[0].Event.ScheduleID != 2 {
		t.Fatalf("parent got %+v", p.Pending)
	}
}

// T12.1: a parent cancels its child and signals it in one checkpoint: the
// child gets the cancel request, then the signal (pg: the child's row is
// locked by lockSignalsAfter, with the targets, in id order).
func testSignalWorkflowCancelChild(t *testing.T, h Harness) {
	s := h.New(t)
	seq := lockSeq{}
	family(t, s, h, seq, false)
	seq.completeWith(t, s, claimID(t, s, h, "c"), &store.Completion{})
	p := wakeTask(t, s, h, "p")
	r := store.SignalRequest{ScheduleID: 2, InstanceID: "c", Name: "hi"}
	seq.completeWith(t, s, p, &store.Completion{ChildCancels: []string{"c"}, Signals: []store.SignalRequest{r}},
		history.Event{Type: history.ChildCancelRequested, ScheduleID: 1}, signalSent(0, r))
	kid := claimID(t, s, h, "c")
	if got := pendingTypes(kid); len(got) != 2 || got[0] != history.ExecutionCancelRequested || got[1] != history.SignalReceived {
		t.Fatalf("child got %v", got)
	}
}

// T12.1: a child signals its parent, or another instance, in the checkpoint
// that ends it: the target gets the signal, and the parent the child's
// outcome after it (pg: the parent's row is locked first, by
// lockSignalsBefore in place of lockParent).
func testSignalWorkflowChildEnds(t *testing.T, h Harness) {
	for _, target := range []string{"p", "x"} {
		t.Run(target, func(t *testing.T) {
			s := h.New(t)
			seq := lockSeq{}
			if target == "x" {
				idle(t, s, h, "x")
			}
			family(t, s, h, seq, false)
			r := store.SignalRequest{ScheduleID: 1, InstanceID: target, Name: "bye", Payload: []byte("2")}
			seq.completeWith(t, s, claimID(t, s, h, "c"),
				&store.Completion{Status: store.StatusCompleted, Output: []byte("9"), Signals: []store.SignalRequest{r}},
				signalSent(0, r), history.Event{Type: history.ExecutionCompleted, Payload: []byte("9")})
			if target == "x" {
				if got := signalPayloads(claimID(t, s, h, "x"), "bye"); len(got) != 1 {
					t.Fatalf("x got %v", got)
				}
			}
			p := claimID(t, s, h, "p")
			want := []history.Type{history.SignalReceived, history.ChildCompleted}
			if target == "x" {
				want = want[1:]
			}
			if got := pendingTypes(p); !slices.Equal(got, want) {
				t.Fatalf("parent got %v, want %v", got, want)
			}
		})
	}
}
