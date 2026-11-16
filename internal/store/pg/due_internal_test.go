package pg

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/testdb"
)

// T10.18 review: the due statements compare with the transaction's now(),
// as the claim pipelined before them does. Work falling due while the
// claim runs (after its now(), before the due statement's real time) is
// not claimed: it must be reported as due (not left to the timed poll), so
// the due statement must not use the real clock. Work another transaction
// made ready after this one began is reported due in a moment: right, it
// was not claimed either.
func TestDueCoversWhatTheClaimSkipped(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO flows.execution (instance_id, workflow, version, queue, status, ready_at)
		VALUES ('i1', 'wf', 1, 'q', 'running', clock_timestamp() + interval '10 ms')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO flows.activity_task (execution_id, instance_id, schedule_id, name, queue, visible_at)
		SELECT id, 'i1', 1, 'a', 'q', clock_timestamp() + interval '10 ms' FROM flows.execution`); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ name, claimable, due, set string }{
		{"workflow", `SELECT count(*) FROM flows.execution WHERE ready_at <= now()`, nextWorkflowDue, `UPDATE flows.execution SET ready_at = $1::timestamptz + interval '10 ms'`},
		{"activity", `SELECT count(*) FROM flows.activity_task WHERE visible_at <= now()`, nextActivityDue, `UPDATE flows.activity_task SET visible_at = $1::timestamptz + interval '10 ms'`},
	} {
		t.Run(c.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			var start time.Time
			if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&start); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, c.set, start); err != nil { // due 10ms after the claim's now()
				t.Fatal(err)
			}
			var n int
			if err := tx.QueryRow(ctx, c.claimable).Scan(&n); err != nil { // the claim's view
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("precondition: %d claimable at the claim's now()", n)
			}
			time.Sleep(30 * time.Millisecond) // the claim runs long; the work falls due
			var due *int64
			if err := tx.QueryRow(ctx, c.due, []string{"q"}).Scan(&due); err != nil {
				t.Fatal(err)
			}
			if n == 0 && due == nil {
				t.Fatal("neither claimed nor reported due: a missed wake-up")
			}
		})
	}
}
