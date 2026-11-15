package pg_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// A row locked by another transaction (a slow completion, an operator's
// SELECT ... FOR UPDATE) is reported busy, not lost, and does not stall the
// renewal of the other leases (saga review item 6).
func TestLockedRowDoesNotStallOtherLeases(t *testing.T) {
	s := newStore(t)
	pool := s.Pool()
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "w", Queue: "q"}); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 10, time.Minute)
	if err != nil || len(tasks) != 2 {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM flows.execution WHERE id = $1 FOR UPDATE`, tasks[0].Lease.ID); err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r, err := s.RenewLeases(rctx, "o", []store.Lease{tasks[0].Lease, tasks[1].Lease}, nil, time.Minute)
	if err != nil {
		t.Fatalf("renewal blocked or failed: %v", err)
	}
	if !slices.Equal(r.BusyWorkflows, []int64{tasks[0].Lease.ID}) || len(r.LostWorkflows) != 0 {
		t.Fatalf("renewal %+v", r)
	}
}

// A terminal completion removes the execution's activity tasks even when
// one of their rows is locked at that moment (an extension in flight): the
// worker learns of the end at its next heartbeat.
func TestEndedExecutionTasksAreRemovedEvenWhenLocked(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "i", Workflow: "w", Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, _ := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	task := tasks[0]
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   []int64{task.Pending[0].ID},
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: "q", ClaimFor: time.Minute}},
		KeepFor:    time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM flows.activity_task WHERE id = $1 FOR UPDATE`, leases[0].ID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
			History: []history.Event{{Seq: 2, Type: history.ExecutionFailed}}, Status: store.StatusFailed,
		})
		done <- err
	}()
	time.Sleep(200 * time.Millisecond) // the completion waits on the task row
	_ = tx.Rollback(ctx)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.ExtendActivityTask(ctx, leases[0], time.Minute); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("task of an ended execution still extendable: %v", err)
	}
}
