package storetest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// signalSent returns the SignalSent event of r at seq.
func signalSent(seq int64, r store.SignalRequest) history.Event {
	return history.Event{
		Seq: seq, Type: history.SignalSent, ScheduleID: r.ScheduleID, Name: r.Name, Ref: r.InstanceID,
		Payload: r.Payload, ArgHash: history.HashArgs(r.Payload),
	}
}

// idle starts execution id and leaves it running with nothing pending.
func idle(t *testing.T, s store.Store, h Harness, id string) {
	t.Helper()
	mustCreate(t, s, id)
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, nil)
}

// sendSignals completes task (its first one: ExecutionStarted pending) with
// signals rs and extra, and returns the completion.
func sendSignals(task store.WorkflowTask, rs []store.SignalRequest, extra store.Completion) *store.Completion {
	hist := []history.Event{task.Pending[0].Event}
	for _, r := range rs {
		hist = append(hist, signalSent(0, r))
	}
	hist = append(hist, extra.History...)
	c := extra
	c.History = hist
	for i := range c.History {
		c.History[i].Seq = task.LastSeq + int64(i) + 1
	}
	c.Consumed = pendingIDs(task)
	c.Signals = rs
	return &c
}

func signalPayloads(task store.WorkflowTask, name string) []string {
	var out []string
	for _, p := range task.Pending {
		if p.Event.Type == history.SignalReceived && p.Event.Name == name {
			out = append(out, string(p.Event.Payload))
		}
	}
	return out
}

// T12.1: the signals a completion sends (Completion.Signals) are delivered
// in its transaction, as Signal delivers them, and answered at once to the
// sender (SignalDelivered, or SignalFailed with the kind of ErrNotFound or
// ErrNotActive), in their order, the signal to itself included.
func testSignalWorkflow(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "c") // ended
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Status: store.StatusCompleted, History: []history.Event{{Type: history.ExecutionCompleted}}})
	idle(t, s, h, "b")
	mustCreate(t, s, "a")
	task = claimOne(t, s, owner, h.TTL)

	rs := []store.SignalRequest{
		{ScheduleID: 1, InstanceID: "b", Name: "go", Payload: []byte(`"p1"`)},
		{ScheduleID: 2, InstanceID: "missing", Name: "go"},
		{ScheduleID: 3, InstanceID: "c", Name: "go"},
		{ScheduleID: 4, InstanceID: "a", Name: "me", Payload: []byte(`"p4"`)},
		{ScheduleID: 5, InstanceID: "b", Name: "go", Payload: []byte(`"p5"`)},
	}
	var next store.WorkflowTask
	c := sendSignals(task, rs, store.Completion{KeepFor: h.TTL, Resume: &next})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if !c.Resumed || next.Lease != task.Lease {
		t.Fatalf("resumed %v: %+v", c.Resumed, next)
	}
	type answer struct {
		typ   history.Type
		sched int32
		ref   string
		kinds string
	}
	var got []answer
	for _, p := range next.Pending {
		ev := p.Event
		if ev.Type == history.SignalReceived {
			got = append(got, answer{ev.Type, 0, ev.Name + "=" + string(ev.Payload), ""})
			continue
		}
		got = append(got, answer{ev.Type, ev.ScheduleID, ev.Ref, ev.ErrKinds})
		if ev.Type == history.SignalFailed && ev.Err == "" {
			t.Errorf("failure without a message: %+v", ev)
		}
	}
	want := []answer{
		{history.SignalDelivered, 1, "b", ""},
		{history.SignalFailed, 2, "missing", store.KindNotFound},
		{history.SignalFailed, 3, "c", store.KindNotActive},
		{history.SignalReceived, 0, `me="p4"`, ""},
		{history.SignalDelivered, 4, "a", ""},
		{history.SignalDelivered, 5, "b", ""},
	}
	if len(got) != len(want) {
		t.Fatalf("answers %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("answer %d: %+v, want %+v (all: %+v)", i, got[i], want[i], got)
		}
	}
	// b got both, in order; c got nothing.
	if err := s.ReleaseWorkflowTask(ctx, next.Lease, 0); err != nil {
		t.Fatal(err)
	}
	var bTask store.WorkflowTask
	for _, x := range claim(t, s, other, h.TTL) {
		switch x.Execution.InstanceID {
		case "b":
			bTask = x
		case "a":
		default:
			t.Fatalf("claimed %s", x.Execution.InstanceID)
		}
	}
	if p := signalPayloads(bTask, "go"); len(p) != 2 || p[0] != `"p1"` || p[1] != `"p5"` {
		t.Fatalf("b received %v", p)
	}
}

// T12.1: a signal to an instance that continued as new reaches its latest
// execution; a signal to itself in a continue-as-new completion moves to
// the next execution (an undelivered signal); answers, to an ended
// execution, are dropped.
func testSignalWorkflowContinueAsNew(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "b")
	task := claimOne(t, s, owner, h.TTL)
	next := newExec("b")
	started(t, s, task, &store.Completion{
		Status: store.StatusContinued, ContinueAsNew: &next,
		History: []history.Event{{Type: history.ExecutionContinuedAsNew}},
	})
	b2 := claimOne(t, s, owner, h.TTL) // its start event
	started(t, s, b2, nil)

	mustCreate(t, s, "a")
	task = claimOne(t, s, owner, h.TTL)
	nextA := newExec("a")
	rs := []store.SignalRequest{
		{ScheduleID: 1, InstanceID: "b", Name: "go", Payload: []byte("1")},
		{ScheduleID: 2, InstanceID: "a", Name: "again", Payload: []byte("2")},
	}
	c := sendSignals(task, rs, store.Completion{
		Status: store.StatusContinued, ContinueAsNew: &nextA,
		History: []history.Event{{Type: history.ExecutionContinuedAsNew}},
	})
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	for _, x := range claim(t, s, owner, h.TTL) {
		switch x.Execution.InstanceID {
		case "b":
			if x.Execution.ID != b2.Execution.ID || len(x.Pending) != 1 || string(x.Pending[0].Event.Payload) != "1" {
				t.Fatalf("b: %+v", x)
			}
		case "a":
			if x.Execution.ID == task.Execution.ID {
				t.Fatal("the ended execution was claimed")
			}
			if got := pendingTypes(x); len(got) != 2 || got[0] != history.ExecutionStarted || got[1] != history.SignalReceived ||
				x.Pending[1].Event.Name != "again" {
				t.Fatalf("a's next execution got %v", got)
			}
		}
	}
}

// T12.1: no dual write. A completion that fails delivers none of its
// signals: refused at the fence (lease lost), or rolled back after its
// statements ran (its end hook failed).
func testSignalWorkflowAtomic(t *testing.T, h Harness) {
	s := h.New(t)
	idle(t, s, h, "b")
	mustCreate(t, s, "a")
	task := claimOne(t, s, owner, h.TTL)
	rs := []store.SignalRequest{{ScheduleID: 1, InstanceID: "b", Name: "go"}}

	stale := task.Lease
	stale.Version++
	if _, err := s.CompleteWorkflowTask(ctx, stale, sendSignals(task, rs, store.Completion{})); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale lease: %v", err)
	}
	if h.SetHook != nil {
		boom := errors.New("boom")
		h.SetHook(s, func(context.Context, store.EndInfo) error { return boom })
		c := sendSignals(task, rs, store.Completion{Status: store.StatusCompleted, History: []history.Event{{Type: history.ExecutionCompleted}}})
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); !errors.Is(err, boom) {
			t.Fatalf("failing hook: %v", err)
		}
		h.SetHook(s, nil)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatalf("delivered by a failed completion: %+v", got)
	}
	if b, err := s.GetExecution(ctx, "b"); err != nil || b.Status != store.StatusRunning {
		t.Fatalf("b: %+v %v", b, err)
	}
	// The same completion with the lease: delivered.
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, sendSignals(task, rs, store.Completion{})); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range claim(t, s, other, h.TTL) {
		found = found || x.Execution.InstanceID == "b" && len(signalPayloads(x, "go")) == 1
	}
	if !found {
		t.Fatal("b did not get the signal")
	}
}

// T12.1: two executions signalling each other in completions running at
// the same time both succeed (no deadlock: the stores lock execution rows
// in id order, pg's lockSignals), round after round.
func testSignalWorkflowMutual(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "a")
	mustCreate(t, s, "b")
	tasks := claim(t, s, owner, h.TTL)
	seq := map[string]int64{}
	peer := map[string]string{"a": "b", "b": "a"}
	const rounds = 15
	for round := range rounds {
		if len(tasks) != 2 {
			t.Fatalf("round %d: %d tasks", round, len(tasks))
		}
		var wg sync.WaitGroup
		errs := make([]error, len(tasks))
		for i, task := range tasks {
			id := task.Execution.InstanceID
			c := &store.Completion{Consumed: pendingIDs(task)}
			for _, p := range task.Pending {
				if round > 0 && p.Event.Type != history.SignalReceived && p.Event.Type != history.SignalDelivered {
					t.Fatalf("round %d: %s got %v", round, id, p.Event.Type)
				}
				seq[id]++
				ev := p.Event
				ev.Seq = seq[id]
				c.History = append(c.History, ev)
			}
			r := store.SignalRequest{ScheduleID: int32(round + 1), InstanceID: peer[id], Name: "ping", Payload: []byte(strconv.Itoa(round))}
			seq[id]++
			c.History = append(c.History, signalSent(seq[id], r))
			c.Signals = []store.SignalRequest{r}
			wg.Go(func() {
				_, errs[i] = s.CompleteWorkflowTask(ctx, task.Lease, c)
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d, %s: %v", round, tasks[i].Execution.InstanceID, err)
			}
		}
		tasks = claim(t, s, owner, h.TTL)
		for _, task := range tasks {
			if p := signalPayloads(task, "ping"); len(p) != 1 || p[0] != strconv.Itoa(round) {
				t.Fatalf("round %d: %s received %v", round, task.Execution.InstanceID, p)
			}
		}
	}
}
