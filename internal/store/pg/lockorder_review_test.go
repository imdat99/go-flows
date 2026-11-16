package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
)

// T10.4 review: a claim over several queues whose first ready row is held
// by another transaction (an in-flight claim or completion) claims from
// the other queues instead of coming back empty. e1 (q1) is ready first
// and locked elsewhere; e2 and e3 (q2) are ready.
func TestClaimOverQueuesPassesLockedRows(t *testing.T) {
	type claimFn func(s *pg.Store) ([]store.WorkflowTask, error)
	claims := map[string]claimFn{
		"claim": func(s *pg.Store) ([]store.WorkflowTask, error) {
			return s.ClaimWorkflowTasks(context.Background(), "o", []string{"q1", "q2"}, 1, time.Minute)
		},
		"due": func(s *pg.Store) ([]store.WorkflowTask, error) {
			tasks, _, err := s.ClaimWorkflowTasksDue(context.Background(), "o", []string{"q1", "q2"}, 1, time.Minute)
			return tasks, err
		},
	}
	for sn, newS := range map[string]func(t *testing.T) *pg.Store{"fast": newStore, "pgx": newStoreWithoutFastPath} {
		for cn, claim := range claims {
			t.Run(sn+"/"+cn, func(t *testing.T) {
				t.Parallel()
				s := newS(t)
				ctx := context.Background()
				for i, e := range [][2]string{{"e1", "q1"}, {"e2", "q2"}, {"e3", "q2"}} {
					if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: e[0], Workflow: "w", Version: 1, Queue: e[1]}); err != nil {
						t.Fatal(err)
					}
					if _, err := s.Pool().Exec(ctx, `UPDATE flows.execution SET ready_at = now() - $2 * interval '1 second'
						WHERE instance_id = $1`, e[0], 10-i); err != nil {
						t.Fatal(err)
					}
				}
				x, err := s.Pool().Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = x.Rollback(ctx) }()
				if _, err := x.Exec(ctx, `SELECT 1 FROM flows.execution WHERE instance_id = 'e1' FOR NO KEY UPDATE`); err != nil {
					t.Fatal(err)
				}
				tasks, err := claim(s)
				if err != nil {
					t.Fatal(err)
				}
				if len(tasks) != 1 || tasks[0].Execution.InstanceID != "e2" {
					t.Fatalf("claimed %+v, want e2", tasks)
				}
			})
		}
	}
}

// T10.4 review: "key order" is byte order (COLLATE "C") everywhere: Go
// sorts lock requests by bytes (sortedLocks), so the statements that lock
// several lock rows must too, whatever the column's collation. The key
// column is given an ICU collation (a database created with a non-C
// locale), where "a" sorts before "B"; in byte order "B" comes first. The
// holder of B and a is ended or claimed while the other side (standing for
// an acquireLock taking B, then a) holds B: the statement must wait for B
// holding nothing.
func TestLockRowsInByteOrder(t *testing.T) {
	type run func(t *testing.T, s *pg.Store) func() error
	runs := map[string]run{}
	for en, end := range lockOrderEnds {
		runs[en] = func(t *testing.T, s *pg.Store) func() error {
			t.Helper()
			task := byteOrderHolder(t, s, false)
			return func() error { return end(s, task, 0) }
		}
	}
	runs["workflow claim"] = func(t *testing.T, s *pg.Store) func() error {
		t.Helper()
		e, _, err := s.CreateExecution(context.Background(), store.NewExecution{InstanceID: "h", Workflow: "w", Version: 1, Queue: "q"})
		if err != nil {
			t.Fatal(err)
		}
		insertByteOrderLocks(t, s, e.ID)
		return func() error {
			tasks, err := s.ClaimWorkflowTasks(context.Background(), "o", []string{"q"}, 1, time.Minute)
			if err == nil && len(tasks) != 1 {
				t.Errorf("claimed %d", len(tasks))
			}
			return err
		}
	}
	runs["activity claim"] = func(t *testing.T, s *pg.Store) func() error {
		t.Helper()
		byteOrderHolder(t, s, true)
		return func() error {
			acts, err := s.ClaimActivityTasks(context.Background(), "o", []string{"q"}, 1, time.Minute)
			if err == nil && len(acts) != 1 {
				t.Errorf("claimed %d activities", len(acts))
			}
			return err
		}
	}
	for sn, newS := range map[string]func(t *testing.T) *pg.Store{"fast": newStore, "pgx": newStoreWithoutFastPath} {
		for rn, r := range runs {
			t.Run(sn+"/"+rn, func(t *testing.T) {
				t.Parallel()
				s := newS(t)
				if _, err := s.Pool().Exec(context.Background(),
					`ALTER TABLE flows.lock ALTER COLUMN key TYPE text COLLATE "und-x-icu"`); err != nil {
					t.Skipf("no ICU collation: %v", err)
				}
				fn := r(t, s)
				whileWaiting(t, s, `SELECT 1 FROM flows.lock WHERE key = 'B' FOR UPDATE`, fn,
					`SELECT 1 FROM flows.lock WHERE key = 'a' FOR UPDATE NOWAIT`)
			})
		}
	}
}

// byteOrderHolder creates execution h (queue q), claims it, and, with act,
// schedules an activity in q and releases it; it then gives h the lock
// rows B and a. It returns the claimed task (released with act).
func byteOrderHolder(t *testing.T, s *pg.Store, act bool) store.WorkflowTask {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "h", Workflow: "w", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %+v %v", tasks, err)
	}
	if act {
		c := &store.Completion{
			Consumed: pendingIDsOf(tasks[0]),
			History: []history.Event{
				{Seq: 1, Type: history.ExecutionStarted},
				{Seq: 2, Type: history.ActivityScheduled, ScheduleID: 1, Name: "a"},
			},
			Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: "q"}},
		}
		if _, err := s.CompleteWorkflowTask(ctx, tasks[0].Lease, c); err != nil {
			t.Fatal(err)
		}
	}
	insertByteOrderLocks(t, s, tasks[0].Execution.ID)
	return tasks[0]
}

// insertByteOrderLocks gives execution id the lock rows a, then B.
func insertByteOrderLocks(t *testing.T, s *pg.Store, id int64) {
	t.Helper()
	for _, k := range []string{"a", "B"} {
		if _, err := s.Pool().Exec(context.Background(), `INSERT INTO flows.lock (key, execution_id, sched, until, ttl_us)
			VALUES ($1, $2, 1, now() + interval '1 hour', 60000000)`, k, id); err != nil {
			t.Fatal(err)
		}
	}
}
