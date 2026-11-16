package pg_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
)

// whileWaiting holds rows (hold, in a transaction of its own), runs run
// until it waits for a row lock, checks that run holds none of the rows
// free asks for (NOWAIT), then lets it finish.
func whileWaiting(t *testing.T, s *pg.Store, hold string, run func() error, free ...string) {
	t.Helper()
	ctx := context.Background()
	x, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = x.Rollback(ctx) }()
	if _, err := x.Exec(ctx, hold); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- run() }()
	waitForLockWait(t, s)
	for _, q := range free {
		if _, err := x.Exec(ctx, `SAVEPOINT free`); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Exec(ctx, q); err != nil {
			t.Errorf("held while it waits: %s: %v", q, err)
			if _, err := x.Exec(ctx, `ROLLBACK TO SAVEPOINT free`); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := x.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("it did not finish")
	}
}

// T10.4: an end (completion, Terminate) takes its lock rows in key order,
// as claims and acquireLock do, not in the order a scan meets them. The
// holder's lock rows are stored b, then a; the other side holds a: the
// end waits for a holding nothing (before, it had deleted b already).
func TestEndsTakeLockRowsInKeyOrder(t *testing.T) {
	for sn, newS := range lockOrderStores {
		for en, end := range lockOrderEnds {
			t.Run(sn+"/"+en, func(t *testing.T) {
				t.Parallel()
				s := newS(t)
				ctx := context.Background()
				if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "h", Workflow: "w", Version: 1, Queue: "q"}); err != nil {
					t.Fatal(err)
				}
				tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
				if err != nil || len(tasks) != 1 {
					t.Fatalf("claim: %+v %v", tasks, err)
				}
				for _, k := range []string{"b", "a"} {
					if _, err := s.Pool().Exec(ctx, `INSERT INTO flows.lock (key, execution_id, sched, until, ttl_us)
						VALUES ($1, $2, 1, now() + interval '1 hour', 60000000)`, k, tasks[0].Execution.ID); err != nil {
						t.Fatal(err)
					}
				}
				whileWaiting(t, s, `SELECT 1 FROM flows.lock WHERE key = 'a' FOR UPDATE`,
					func() error { return end(s, tasks[0], 0) },
					`SELECT 1 FROM flows.lock WHERE key = 'b' FOR UPDATE NOWAIT`)
				var n int
				if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM flows.lock`).Scan(&n); err != nil || n != 0 {
					t.Fatalf("%d lock rows left: %v", n, err)
				}
			})
		}
	}
}

// T10.4: Unstick locks the execution rows it updates by id (an ancestor's
// before a descendant's, as lockParent), not in the order its scan meets
// them (here the sticky index: e2 first). The other side holds e1.
func TestUnstickLocksRowsByID(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, id := range []string{"e1", "e2"} {
		if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "w", Version: 1, Queue: "q"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`UPDATE flows.execution SET sticky_owner = 'o', sticky_until = now() + interval '1 hour' WHERE instance_id = 'e2'`,
		`UPDATE flows.execution SET sticky_owner = 'o', sticky_until = now() + interval '2 hours' WHERE instance_id = 'e1'`,
	} {
		if _, err := s.Pool().Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	whileWaiting(t, s, `SELECT 1 FROM flows.execution WHERE instance_id = 'e1' FOR NO KEY UPDATE`,
		func() error { return s.Unstick(ctx, "o") },
		`SELECT 1 FROM flows.execution WHERE instance_id = 'e2' FOR NO KEY UPDATE NOWAIT`)
	var n int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM flows.execution WHERE sticky_owner <> ''`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d executions still sticky: %v", n, err)
	}
}

// T10.4: a claim over several queues locks only the executions it claims,
// and their lock rows: not up to its limit in each queue. Queues q1 and q2,
// limit 1: e1 (q1, ready first) is claimed; e2 (q2) and its lock row kb
// stay free while the claim waits for e1's lock row ka.
func TestClaimLocksOnlyWhatItClaims(t *testing.T) {
	for sn, newS := range map[string]func(t *testing.T) *pg.Store{"fast": newStore, "pgx": newStoreWithoutFastPath} {
		t.Run(sn, func(t *testing.T) {
			t.Parallel()
			s := newS(t)
			ctx := context.Background()
			for _, e := range [][2]string{{"e1", "q1"}, {"e2", "q2"}} {
				x, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: e[0], Workflow: "w", Version: 1, Queue: e[1]})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.Pool().Exec(ctx, `INSERT INTO flows.lock (key, execution_id, sched, until, ttl_us)
					VALUES ($1, $2, 1, now() - interval '1 second', 60000000)`, "k"+e[0][1:], x.ID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Pool().Exec(ctx, `UPDATE flows.execution SET ready_at = now() - interval '1 minute' WHERE instance_id = 'e1'`); err != nil {
				t.Fatal(err)
			}
			var tasks []store.WorkflowTask
			whileWaiting(t, s, `SELECT 1 FROM flows.lock WHERE key = 'k1' FOR UPDATE`,
				func() error {
					var err error
					tasks, err = s.ClaimWorkflowTasks(ctx, "o", []string{"q1", "q2"}, 1, time.Minute)
					return err
				},
				`SELECT 1 FROM flows.lock WHERE key = 'k2' FOR UPDATE NOWAIT`,
				`SELECT 1 FROM flows.execution WHERE instance_id = 'e2' FOR NO KEY UPDATE NOWAIT`)
			if len(tasks) != 1 || tasks[0].Execution.InstanceID != "e1" {
				t.Fatalf("claimed %+v", tasks)
			}
		})
	}
}

// T10.4: a claim over several queues (claimWorkflowQueues) claims in
// readiness order across them, up to its limit.
func TestClaimOverQueuesInReadinessOrder(t *testing.T) {
	for sn, newS := range map[string]func(t *testing.T) *pg.Store{"fast": newStore, "pgx": newStoreWithoutFastPath} {
		t.Run(sn, func(t *testing.T) {
			t.Parallel()
			s := newS(t)
			ctx := context.Background()
			// a, c in q1; b, d in q2; ready in alphabetical order.
			for i, e := range [][2]string{{"a", "q1"}, {"b", "q2"}, {"c", "q1"}, {"d", "q2"}} {
				if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: e[0], Workflow: "w", Version: 1, Queue: e[1]}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Pool().Exec(ctx, `UPDATE flows.execution SET ready_at = now() - $2 * interval '1 second'
					WHERE instance_id = $1`, e[0], 10-i); err != nil {
					t.Fatal(err)
				}
			}
			ids := func(limit int) []string {
				t.Helper()
				tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q2", "q1"}, limit, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				var out []string
				for _, tk := range tasks {
					out = append(out, tk.Execution.InstanceID)
				}
				slices.Sort(out)
				return out
			}
			if got := ids(3); !slices.Equal(got, []string{"a", "b", "c"}) {
				t.Fatalf("first claim %v", got)
			}
			if got := ids(3); !slices.Equal(got, []string{"d"}) {
				t.Fatalf("second claim %v", got)
			}
		})
	}
}
