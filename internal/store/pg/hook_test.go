package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// The end hook writes in the ending transaction: its rows commit with the
// end, and vanish with it when the hook fails (a transactional outbox).
func TestEndHookIsAtomic(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if _, err := s.Pool().Exec(ctx, `CREATE TABLE outbox (instance text, status text)`); err != nil {
		t.Fatal(err)
	}
	failNext := true
	s.SetHook(func(ctx context.Context, tx pgx.Tx, e store.EndInfo) error {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox VALUES ($1, $2)`, e.InstanceID, string(e.Status)); err != nil {
			return err
		}
		if failNext {
			failNext = false
			return errors.New("refused")
		}
		return nil
	})
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "i1", Workflow: "wf", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %v %v", tasks, err)
	}
	task := tasks[0]
	c := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionCompleted}},
		Consumed: []int64{task.Pending[0].ID}, Status: store.StatusCompleted,
	}
	count := func() (n int) {
		_ = s.Pool().QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n)
		return n
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err == nil {
		t.Fatal("end committed despite the hook's error")
	}
	if n := count(); n != 0 {
		t.Fatalf("the failed hook's write survived: %d rows", n)
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 1 {
		t.Fatalf("outbox rows %d", n)
	}
	// A stale lease never reaches the hook.
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale lease: %v", err)
	}
	if n := count(); n != 1 {
		t.Fatalf("outbox rows after a stale completion %d", n)
	}
}

// Review of T5.5: a hook error is the hook's, whatever SQLSTATE it
// carries: never mistaken for a lost lease, pending events or an invalid
// completion (the store's own signals use such codes).
func TestEndHookErrorsAreNotStoreSignals(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	codes := []string{"22012", "2201E", "22P02", "23505"}
	calls := 0
	s.SetHook(func(context.Context, pgx.Tx, store.EndInfo) error {
		calls++
		if calls <= len(codes) {
			return &pgconn.PgError{Code: codes[calls-1], Message: "outbox"}
		}
		return nil
	})
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "i1", Workflow: "wf", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %v %v", tasks, err)
	}
	c := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionCompleted}},
		Consumed: []int64{tasks[0].Pending[0].ID}, Status: store.StatusCompleted,
	}
	for _, code := range codes {
		_, err := s.CompleteWorkflowTask(ctx, tasks[0].Lease, c)
		if err == nil || errors.Is(err, store.ErrLeaseLost) || errors.Is(err, store.ErrPendingEvents) || errors.Is(err, store.ErrInvalidCompletion) {
			t.Fatalf("hook error %s reported as %v", code, err)
		}
	}
	if _, err := s.CompleteWorkflowTask(ctx, tasks[0].Lease, c); err != nil {
		t.Fatal(err)
	}
	// Terminate too.
	calls = 0
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "i2", Workflow: "wf", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Terminate(ctx, "i2", "x"); err == nil || errors.Is(err, store.ErrNotActive) || strings.Contains(err.Error(), "continuing") {
		t.Fatalf("terminate with a failing hook: %v", err)
	}
}
