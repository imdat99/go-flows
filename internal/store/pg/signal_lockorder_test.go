package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// T12.1: a completion that signals other executions locks their rows and
// its own in id order (pg.lockSignalsBefore, then the fence, then
// pg.lockSignalsAfter). Execution b (created after a: a higher id)
// signalling a waits for a's row before it takes its own, so a's
// completion signalling b at the same time (its own row, then b's) cannot
// close a cycle. Another transaction holds a's row (standing for a's
// completion); while b's completion waits for it, b's row must be free.
func TestSignalLocksRowsInIDOrder(t *testing.T) {
	for sn, newS := range lockOrderStores {
		for _, ending := range []bool{false, true} {
			name := sn + "/running"
			if ending {
				name = sn + "/ending"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				s := newS(t)
				ctx := context.Background()
				claim := func(id string) store.WorkflowTask {
					t.Helper()
					tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
					if err != nil || len(tasks) != 1 || tasks[0].Execution.InstanceID != id {
						t.Fatalf("claim %s: %+v %v", id, tasks, err)
					}
					return tasks[0]
				}
				for _, id := range []string{"a", "b"} {
					if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "w", Version: 1, Queue: "q"}); err != nil {
						t.Fatal(err)
					}
				}
				ta := claim("a")
				if _, err := s.CompleteWorkflowTask(ctx, ta.Lease, &store.Completion{
					History: []history.Event{{Seq: 1, Type: history.ExecutionStarted}}, Consumed: pendingIDsOf(ta),
				}); err != nil {
					t.Fatal(err)
				}
				tb := claim("b")
				r := store.SignalRequest{ScheduleID: 1, InstanceID: "a", Name: "go"}
				c := &store.Completion{
					History: []history.Event{
						{Seq: 1, Type: history.ExecutionStarted},
						{Seq: 2, Type: history.SignalSent, ScheduleID: 1, Name: "go", Ref: "a", ArgHash: history.HashArgs(nil)},
					},
					Consumed: pendingIDsOf(tb),
					Signals:  []store.SignalRequest{r},
				}
				if ending {
					c.Status = store.StatusCompleted
					c.History = append(c.History, history.Event{Seq: 3, Type: history.ExecutionCompleted})
				}
				x, err := s.Pool().Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = x.Rollback(ctx) }()
				if _, err := x.Exec(ctx, `SELECT 1 FROM flows.execution WHERE instance_id = 'a' FOR NO KEY UPDATE`); err != nil {
					t.Fatal(err)
				}
				errc := make(chan error, 1)
				go func() {
					_, err := s.CompleteWorkflowTask(ctx, tb.Lease, c)
					errc <- err
				}()
				waitForLockWait(t, s)
				if _, err := x.Exec(ctx, `SELECT 1 FROM flows.execution WHERE instance_id = 'b' FOR NO KEY UPDATE NOWAIT`); err != nil {
					t.Fatalf("b's row is held while its completion waits for a's: %v", err)
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
				if task := claim("a"); len(task.Pending) != 1 || task.Pending[0].Event.Type != history.SignalReceived {
					t.Fatalf("a: %+v", task.Pending)
				}
			})
		}
	}
}
