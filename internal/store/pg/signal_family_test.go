package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
)

// sigFixture drives executions of one store by hand (instance id order is
// creation order, so id order).
type sigFixture struct {
	t   *testing.T
	s   *pg.Store
	seq map[string]int64
}

func newSigFixture(t *testing.T, s *pg.Store) *sigFixture {
	t.Helper()
	return &sigFixture{t: t, s: s, seq: map[string]int64{}}
}

// claim claims the one ready task, of instance id.
func (f *sigFixture) claim(id string) store.WorkflowTask {
	f.t.Helper()
	tasks, err := f.s.ClaimWorkflowTasks(context.Background(), "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 || tasks[0].Execution.InstanceID != id {
		f.t.Fatalf("claim %s: %+v %v", id, tasks, err)
	}
	return tasks[0]
}

// completion records task's pending events, then evs, into c.
func (f *sigFixture) completion(task store.WorkflowTask, c *store.Completion, evs ...history.Event) *store.Completion {
	id := task.Execution.InstanceID
	c.Consumed = pendingIDsOf(task)
	all := make([]history.Event, 0, len(task.Pending)+len(evs))
	for _, p := range task.Pending {
		all = append(all, p.Event)
	}
	for _, ev := range append(all, evs...) {
		f.seq[id]++
		ev.Seq = f.seq[id]
		c.History = append(c.History, ev)
	}
	return c
}

func (f *sigFixture) complete(task store.WorkflowTask, c *store.Completion, evs ...history.Event) {
	f.t.Helper()
	if _, err := f.s.CompleteWorkflowTask(context.Background(), task.Lease, f.completion(task, c, evs...)); err != nil {
		f.t.Fatalf("complete %s: %v", task.Execution.InstanceID, err)
	}
}

// start creates instance id and runs its first task.
func (f *sigFixture) start(id string) {
	f.t.Helper()
	if _, _, err := f.s.CreateExecution(context.Background(), store.NewExecution{InstanceID: id, Workflow: "w", Version: 1, Queue: "q"}); err != nil {
		f.t.Fatal(err)
	}
	f.complete(f.claim(id), &store.Completion{})
}

// family starts parent p, which starts child c (schedule id 1); both idle.
func (f *sigFixture) family() {
	f.t.Helper()
	if _, _, err := f.s.CreateExecution(context.Background(), store.NewExecution{InstanceID: "p", Workflow: "w", Version: 1, Queue: "q"}); err != nil {
		f.t.Fatal(err)
	}
	f.complete(f.claim("p"), &store.Completion{Children: []store.NewExecution{{InstanceID: "c", Workflow: "w", Version: 1, Queue: "q", ParentSched: 1}}},
		history.Event{Type: history.ChildScheduled, ScheduleID: 1, Name: "w", Version: 1, Ref: "c", Queue: "q"})
	f.complete(f.claim("c"), &store.Completion{})
}

// wake makes id ready (a signal) and claims it.
func (f *sigFixture) wake(id string) store.WorkflowTask {
	f.t.Helper()
	if err := f.s.Signal(context.Background(), id, "wake", nil); err != nil {
		f.t.Fatal(err)
	}
	return f.claim(id)
}

func sent(r store.SignalRequest) history.Event {
	return history.Event{Type: history.SignalSent, ScheduleID: r.ScheduleID, Name: r.Name, Ref: r.InstanceID, ArgHash: history.HashArgs(r.Payload)}
}

// holdWhile holds row hold in another transaction, runs complete in the
// background, waits until it waits for a lock, checks that each of free
// (FOR NO KEY UPDATE NOWAIT) is not held by it, then lets it finish.
func holdWhile(t *testing.T, s *pg.Store, hold string, free []string, complete func() error) {
	t.Helper()
	ctx := context.Background()
	x, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Rollback(ctx) }()
	if _, err := x.Exec(ctx, `SELECT 1 FROM flows.execution WHERE instance_id = $1 AND status = 'running' FOR NO KEY UPDATE`, hold); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- complete() }()
	waitForLockWait(t, s)
	for _, id := range free {
		if _, err := x.Exec(ctx, `SELECT 1 FROM flows.execution WHERE instance_id = $1 AND status = 'running' FOR NO KEY UPDATE NOWAIT`, id); err != nil {
			t.Fatalf("%s's row is held while the completion waits for %s's: %v", id, hold, err)
		}
	}
	if err := x.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the completion did not finish")
	}
}

// T12.1 and T9.5: a child that ends while signalling an instance other
// than its parent still locks its parent's row before its own
// (lockSignalsBefore's parent branch, standing for lockParent): holding
// the parent's row, the child's own must stay free while its completion
// waits.
func TestSignalChildEndLocksParentFirst(t *testing.T) {
	for sn, newS := range lockOrderStores {
		t.Run(sn, func(t *testing.T) {
			t.Parallel()
			s := newS(t)
			f := newSigFixture(t, s)
			f.family()
			f.start("x") // a target of a higher id than c
			task := f.wake("c")
			r := store.SignalRequest{ScheduleID: 1, InstanceID: "x", Name: "bye"}
			c := f.completion(task, &store.Completion{Status: store.StatusCompleted, Signals: []store.SignalRequest{r}},
				sent(r), history.Event{Type: history.ExecutionCompleted})
			holdWhile(t, s, "p", []string{"c"}, func() error {
				_, err := s.CompleteWorkflowTask(context.Background(), task.Lease, c)
				return err
			})
		})
	}
}

// T12.1: a parent that cancels its child and signals an instance of a
// higher id locks the child's row and the target's in id order
// (lockSignalsAfter's children branch): holding the child's row, the
// target's must stay free while the parent's completion waits.
func TestSignalLocksChildrenWithTargets(t *testing.T) {
	for sn, newS := range lockOrderStores {
		t.Run(sn, func(t *testing.T) {
			t.Parallel()
			s := newS(t)
			f := newSigFixture(t, s)
			f.family()
			f.start("x")
			task := f.wake("p")
			r := store.SignalRequest{ScheduleID: 2, InstanceID: "x", Name: "hi"}
			c := f.completion(task, &store.Completion{ChildCancels: []string{"c"}, Signals: []store.SignalRequest{r}},
				history.Event{Type: history.ChildCancelRequested, ScheduleID: 1}, sent(r))
			holdWhile(t, s, "c", []string{"x"}, func() error {
				_, err := s.CompleteWorkflowTask(context.Background(), task.Lease, c)
				return err
			})
		})
	}
}

// T12.1: a target that continues as new while the sender waits for its
// row gets the signal in its new execution (sendSignals, a statement after
// the lock wait, sees it), as Engine.Signal would deliver it; the sender's
// answer is SignalDelivered.
func TestSignalFollowsContinueAsNewDuringLockWait(t *testing.T) {
	for sn, newS := range lockOrderStores {
		t.Run(sn, func(t *testing.T) {
			t.Parallel()
			s := newS(t)
			ctx := context.Background()
			f := newSigFixture(t, s)
			f.start("a")
			f.start("b")
			task := f.wake("a")
			r := store.SignalRequest{ScheduleID: 1, InstanceID: "b", Name: "hi"}
			var next store.WorkflowTask
			c := f.completion(task, &store.Completion{Signals: []store.SignalRequest{r}, KeepFor: time.Minute, Resume: &next}, sent(r))
			x, err := s.Pool().Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = x.Rollback(ctx) }()
			// b continues as new in x (as its completion would).
			if _, err := x.Exec(ctx, `UPDATE flows.execution SET status = 'continued', ended_at = now(), ready_at = NULL
				WHERE instance_id = 'b' AND status = 'running'`); err != nil {
				t.Fatal(err)
			}
			if _, err := x.Exec(ctx, `INSERT INTO flows.execution (instance_id, workflow, version, queue, status)
				VALUES ('b', 'w', 1, 'q', 'running')`); err != nil {
				t.Fatal(err)
			}
			errc := make(chan error, 1)
			go func() {
				_, err := s.CompleteWorkflowTask(ctx, task.Lease, c)
				errc <- err
			}()
			waitForLockWait(t, s)
			if err := x.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
			if !c.Resumed || len(next.Pending) != 1 || next.Pending[0].Event.Type != history.SignalDelivered {
				t.Fatalf("answer: resumed %v, %+v", c.Resumed, next.Pending)
			}
			var n int
			if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM flows.pending_event p JOIN flows.execution e ON e.id = p.execution_id
				WHERE e.instance_id = 'b' AND e.status = 'running' AND p.type = $1`, int16(history.SignalReceived)).Scan(&n); err != nil || n != 1 {
				t.Fatalf("b's new execution has %d signals (%v)", n, err)
			}
		})
	}
}
