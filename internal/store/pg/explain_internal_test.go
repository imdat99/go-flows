package pg

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/imdat99/go-flows/internal/testdb"
)

// GW-8: the claim statements can use their partial indexes (go-workflows
// scanned every active instance), and their lock extension the lock index.
// With seq scans disabled, the plan must name each index; a planner that
// cannot use one falls back to a seq scan and fails the test.
func TestGW8_ClaimsUseTheirIndexes(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, sql string
		indexes   []string
		args      []any
	}{
		{"workflow claim", claimWorkflow, []string{"execution_ready_idx", "lock_execution"}, []any{[]string{"q"}, "o", 10, int64(1e6)}},
		{"activity claim", claimActivities, []string{"activity_task_ready_idx", "lock_execution"}, []any{[]string{"q"}, "o", 10, int64(1e6)}},
		{
			"latest execution", `SELECT ` + executionColumns + ` FROM flows.execution WHERE instance_id = $1 ORDER BY id DESC LIMIT 1`,
			[]string{"execution_instance_idx"},
			[]any{"i"},
		},
	}
	for _, c := range cases {
		rows, err := conn.Query(ctx, "EXPLAIN "+c.sql, c.args...)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(line + "\n")
		}
		rows.Close()
		for _, index := range c.indexes {
			if !strings.Contains(plan.String(), index) {
				t.Errorf("%s does not use %s:\n%s", c.name, index, plan.String())
			}
		}
	}
}

// T8.4 review (GW-8 at scale): with a large ready backlog, a claim reads
// about as many rows as it claims, in index order: no sort of the whole
// backlog (`queue = ANY($1)` alone cannot use the index order), and the
// pending events of the claimed executions come from their index, not from
// a scan of every future timer. A purge reads about as many rows as it
// deletes (it walks up from each candidate, not down from every active
// execution). Generic plans are forced, as prepared statements end up
// using them.
func TestGW8_ClaimsReadOnlyWhatTheyClaim(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const n = 20000
	for _, sql := range []string{
		`INSERT INTO flows.execution (instance_id, workflow, version, queue, status, ready_at)
		 SELECT 'i' || g, 'w', 1, 'q', 'running', now() - interval '1 second' FROM generate_series(1, 20000) g`,
		`INSERT INTO flows.activity_task (execution_id, instance_id, schedule_id, name, queue, visible_at)
		 SELECT id, instance_id, 1, 'a', 'q', now() - interval '1 second' FROM flows.execution`,
		`INSERT INTO flows.pending_event (execution_id, visible_at, type)
		 SELECT id, now() + interval '1 hour', 3 FROM flows.execution`,
		// Ended executions, half of them children of a running one (kept).
		`INSERT INTO flows.execution (instance_id, workflow, version, queue, status, ended_at, parent_id)
		 SELECT 'e' || g, 'w', 1, 'q', 'completed', now() - interval '1 day',
		        CASE WHEN g % 2 = 0 THEN (SELECT min(id) FROM flows.execution) ELSE 0 END
		 FROM generate_series(1, 20000) g`,
		`ANALYZE`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	rowsRe := regexp.MustCompile(`actual time=[0-9.]+\.\.[0-9.]+ rows=([0-9]+)`)
	maxRows := func(t *testing.T, sql string, args ...any) (int, string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SET LOCAL plan_cache_mode = force_generic_plan`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `PREPARE p AS `+sql); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.Query(ctx, `EXPLAIN (ANALYZE) EXECUTE p`+args[0].(string))
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		most := 0
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(line + "\n")
			if m := rowsRe.FindStringSubmatch(line); m != nil {
				if v, _ := strconv.Atoi(m[1]); v > most {
					most = v
				}
			}
		}
		rows.Close()
		if _, err := tx.Exec(ctx, `DEALLOCATE p`); err != nil { // session-level: a rollback keeps it
			t.Fatal(err)
		}
		return most, plan.String()
	}
	for _, c := range []struct{ name, sql, args string }{
		{"workflow claim", claimWorkflow, `('{q}', 'o', 10, 60000000)`},
		{"activity claim", claimActivities, `('{q}', 'o', 10, 60000000)`},
		{"purge", purgeEnded, `(0, 10)`},
	} {
		if most, plan := maxRows(t, c.sql, c.args); most > 100 {
			t.Errorf("%s reads %d rows of a %d backlog to claim 10:\n%s", c.name, most, n, plan)
		}
	}
}
