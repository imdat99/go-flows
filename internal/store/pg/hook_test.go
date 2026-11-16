package pg_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
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
	if _, err := s.Terminate(ctx, "i2", "x"); err == nil || errors.Is(err, store.ErrNotActive) || strings.Contains(err.Error(), "continuing") {
		t.Fatalf("terminate with a failing hook: %v", err)
	}
}

// queryLog records the statements a pool sends outside batches (each is a
// round trip of its own) while on.
type queryLog struct {
	mu  sync.Mutex
	on  bool
	sql []string
}

func (q *queryLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.on {
		q.sql = append(q.sql, strings.TrimSpace(d.SQL))
	}
	return ctx
}

func (*queryLog) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// record turns recording on, runs fn, and returns the statements sent.
func (q *queryLog) record(fn func()) []string {
	q.mu.Lock()
	q.on, q.sql = true, nil
	q.mu.Unlock()
	fn()
	q.mu.Lock()
	defer q.mu.Unlock()
	q.on = false
	return q.sql
}

// newLoggedStore is a store whose pool logs its statements (pgx path).
func newLoggedStore(t *testing.T) (*pg.Store, *queryLog) {
	t.Helper()
	base := testdb.New(t)
	if err := pg.Migrate(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	cfg := base.Config()
	ql := &queryLog{}
	cfg.ConnConfig.Tracer = ql
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pg.New(pool), ql
}

// T10.9: an end under a hook is BEGIN, one batch, the hook, COMMIT: the
// execution's identity for the hook comes back from the batch, not from a
// SELECT of its own (one round trip less). The hook sees the same values.
func TestHookedEndsReadNothingMore(t *testing.T) {
	s, ql := newLoggedStore(t)
	ctx := context.Background()
	var seen []store.EndInfo
	s.SetHook(func(_ context.Context, _ pgx.Tx, e store.EndInfo) error {
		seen = append(seen, e)
		return nil
	})
	for _, id := range []string{"i1", "i2"} {
		if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "wf", Version: 3, Queue: "q"}); err != nil {
			t.Fatal(err)
		}
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
	want := []string{"begin", "commit"}
	sent := ql.record(func() {
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
			t.Fatal(err)
		}
	})
	if !slices.Equal(sent, want) {
		t.Fatalf("completion sent %q, want %q (plus one batch)", sent, want)
	}
	other := "i2"
	if task.Execution.InstanceID == "i2" {
		other = "i1"
	}
	var ended store.EndInfo
	sent = ql.record(func() {
		if ended, err = s.Terminate(ctx, other, "stop"); err != nil {
			t.Fatal(err)
		}
	})
	if !slices.Equal(sent, want) {
		t.Fatalf("terminate sent %q, want %q (plus one batch)", sent, want)
	}
	if len(seen) != 2 {
		t.Fatalf("hook calls %+v", seen)
	}
	for i, id := range []string{task.Execution.InstanceID, other} {
		e, err := s.GetExecution(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got := seen[i]; got.ExecutionID != e.ID || got.InstanceID != id || got.Workflow != "wf" || got.Version != 3 {
			t.Fatalf("hook saw %+v for %+v", got, e)
		}
	}
	// Terminate returns what the hook saw.
	if ended != seen[1] || ended.Status != store.StatusTerminated || ended.Err != "stop" {
		t.Fatalf("Terminate returned %+v, hook saw %+v", ended, seen[1])
	}
}

// Review of T10.9: a hooked park keeping its lease with a resume reads the
// identity statement before the resume's (the batch order holds).
func TestHookedParkWithResume(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	var seen []store.EndInfo
	s.SetHook(func(_ context.Context, _ pgx.Tx, e store.EndInfo) error {
		seen = append(seen, e)
		return nil
	})
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "p1", Workflow: "wf", Version: 2, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %v %v", tasks, err)
	}
	c := &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed: []int64{tasks[0].Pending[0].ID}, Status: store.StatusNeedsAttention, Err: "parked",
		KeepFor: time.Minute, Resume: &store.WorkflowTask{},
	}
	if _, err := s.CompleteWorkflowTask(ctx, tasks[0].Lease, c); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].InstanceID != "p1" || seen[0].Workflow != "wf" || seen[0].Version != 2 || seen[0].Status != store.StatusNeedsAttention {
		t.Fatalf("hook saw %+v", seen)
	}
}
