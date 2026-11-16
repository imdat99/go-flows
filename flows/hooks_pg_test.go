package flows_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/testdb"
)

var (
	pgNDChanged atomic.Bool
	pgNDWF      = flows.DefineWorkflow("pgtest.nd-hook", 1, func(c flows.Ctx, _ int) (int, error) {
		if !pgNDChanged.Load() {
			if _, err := pick.Run(c, item{"x", 1}); err != nil {
				return 0, err
			}
		}
		return flows.Await[int](c, "go", time.Hour)
	})
)

// T10.14: on Postgres, a park for non-determinism runs the run hook in
// the transaction that parks the execution.
func TestPostgresRunHookSeesNonDeterminismPark(t *testing.T) {
	t.Cleanup(func() { pgNDChanged.Store(false) })
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE outbox (instance text PRIMARY KEY, status text)`); err != nil {
		t.Fatal(err)
	}
	// No cache: the task after the deploy replays the history.
	e, err := flows.New(pool, flows.Options{InstanceID: "pg-nd-hook", PollMax: 20 * time.Millisecond, CacheSize: -1, Hook: outbox{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(ctx) })
	h, err := pgNDWF.Start(ctx, e, "nd-park", 0)
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Wait until the activity completed and the workflow awaits the signal.
	for {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM flows.history h JOIN flows.execution x ON x.id = h.execution_id
			WHERE x.instance_id = 'nd-park' AND h.type = $1`, int16(history.TimerScheduled)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		select {
		case <-wctx.Done():
			t.Fatal("the workflow never awaited the signal")
		case <-time.After(10 * time.Millisecond):
		}
	}
	pgNDChanged.Store(true) // a deploy that removed the activity call
	if err := e.Signal(ctx, "nd-park", "go", 1); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(wctx)
	var re *flows.RunError
	if !errors.As(err, &re) || re.Status != flows.StatusNeedsAttention || !errors.Is(err, flows.ErrNonDeterministic) {
		t.Fatalf("want a park for non-determinism, got %v", err)
	}
	rows, err := pool.Query(ctx, `SELECT instance || '=' || status FROM outbox ORDER BY instance`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"nd-park=needs_attention"}) {
		t.Fatalf("outbox %v", got)
	}
}
