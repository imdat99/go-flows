package pg

import (
	"context"
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
