package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// An activity result delivered while the lease holder is releasing must not
// be lost: the release must see it, or the delivery must wake the released
// execution. A third transaction holding a share lock widens the window in
// which the release statement waits for the row (found in review of T2.8).
func TestNoLostWakeWhileReleasing(t *testing.T) {
	for _, release := range []struct {
		name string
		do   func(ctx context.Context, s *storeT, task store.WorkflowTask) error
	}{
		{"ReleaseWorkflowTask", func(ctx context.Context, s *storeT, task store.WorkflowTask) error {
			return s.ReleaseWorkflowTask(ctx, task.Lease, 0)
		}},
		{"CompleteWorkflowTask", func(ctx context.Context, s *storeT, task store.WorkflowTask) error {
			_, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{})
			return err
		}},
	} {
		t.Run(release.name, func(t *testing.T) {
			s := newStore(t)
			// Every wait is bounded: a stuck statement fails the test, it
			// does not hang it.
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "x", Workflow: "w", Queue: "q"}); err != nil {
				t.Fatal(err)
			}
			tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Hour)
			if err != nil || len(tasks) != 1 {
				t.Fatal(err)
			}
			task := tasks[0]
			leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
				History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ActivityScheduled, ScheduleID: 1, Name: "a"}},
				Consumed:   []int64{task.Pending[0].ID},
				Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: "q", ClaimFor: time.Hour}},
				KeepFor:    time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			task.LastSeq = 2
			z, err := s.Pool().Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// A failure before the commit: z ends, so the pool can close.
			defer func() { _ = z.Rollback(context.Background()) }()
			if _, err := z.Exec(ctx, `SELECT 1 FROM flows.execution WHERE id = $1 FOR SHARE`, task.Lease.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- release.do(ctx, s, task) }()
			waitLockWaiters(ctx, t, s, 1, nil) // the release waits for z
			actDone := make(chan error, 1)
			go func() {
				_, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}})
				actDone <- err
			}()
			// The delivery waits too (for z or the release), unless it is
			// through already.
			waitLockWaiters(ctx, t, s, 2, actDone)
			if err := z.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatalf("release: %v", err)
			}
			if err := <-actDone; err != nil {
				t.Fatalf("activity completion: %v", err)
			}
			got, err := s.ClaimWorkflowTasks(ctx, "p", []string{"q"}, 1, time.Minute)
			if err != nil || len(got) != 1 || len(got[0].Pending) != 1 {
				t.Fatalf("lost wake-up: claim %+v %v", got, err)
			}
		})
	}
}

// waitLockWaiters waits until n sessions of the test database wait for a
// lock, or (with through) until through has a value (put back for its
// reader).
func waitLockWaiters(ctx context.Context, t *testing.T, s *storeT, n int, through chan error) {
	t.Helper()
	for {
		var waiting int
		err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
		if err != nil {
			t.Fatalf("waiting for %d lock waiters: %v", n, err)
		}
		if waiting >= n {
			return
		}
		if through != nil {
			select {
			case err := <-through:
				through <- err
				return
			default:
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %d lock waiters: %v", n, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
