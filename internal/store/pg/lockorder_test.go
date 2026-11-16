package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

// newStoreWithoutFastPath is a store on the pgx path.
func newStoreWithoutFastPath(t *testing.T) *pg.Store {
	t.Helper()
	base := testdb.New(t)
	if err := pg.Migrate(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	cfg := base.Config()
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pg.New(pool)
}

// T9.5: every transaction locks rows in one order (pg.lockParent):
// execution rows, ancestor before descendant; then activity task rows; then
// lock rows. Each case holds, in another transaction, the row an engine
// transaction would hold first (standing for it), ends an execution, waits
// until the end waits for that row, and checks that the end holds none of
// the rows the other side would ask for next (NOWAIT). Before, each was a
// deadlock broken only by Postgres' detection (deadlock_timeout):
//   - child ends (its row, its lock rows, then the parent's row) against
//     the parent's completion (its row, then the same lock in acquireLock);
//   - parent ends (its row, its lock rows, then its child's row in
//     cancelChildrenOf) against the child's completion (its row, then the
//     same lock);
//   - an execution ends (lock rows and task rows in one statement) against
//     an activity claim (the task row, then the execution's lock rows).
func TestEndsLockRowsInOrder(t *testing.T) {
	type tcase struct {
		holder string // takes lock k (and has an activity task, for "task")
		hold   string // the row the other transaction holds
		free   []string
	}
	cases := map[string]tcase{
		"child ends": {holder: "c", hold: `SELECT 1 FROM flows.execution WHERE instance_id = 'p' FOR NO KEY UPDATE`, free: []string{
			`SELECT 1 FROM flows.lock WHERE key = 'k' FOR UPDATE NOWAIT`,
			`SELECT 1 FROM flows.execution WHERE instance_id = 'c' FOR NO KEY UPDATE NOWAIT`,
		}},
		"parent ends": {holder: "p", hold: `SELECT 1 FROM flows.execution WHERE instance_id = 'c' FOR NO KEY UPDATE`, free: []string{
			`SELECT 1 FROM flows.lock WHERE key = 'k' FOR UPDATE NOWAIT`,
		}},
		"task": {holder: "c", hold: `SELECT 1 FROM flows.activity_task FOR UPDATE`, free: []string{
			`SELECT 1 FROM flows.lock WHERE key = 'k' FOR UPDATE NOWAIT`,
		}},
	}
	ends := map[string]func(s *pg.Store, task store.WorkflowTask, seq int64) error{
		"complete": func(s *pg.Store, task store.WorkflowTask, seq int64) error {
			c := &store.Completion{Consumed: pendingIDsOf(task), Status: store.StatusCompleted, Output: []byte("1")}
			for _, p := range task.Pending {
				seq++
				ev := p.Event
				ev.Seq = seq
				c.History = append(c.History, ev)
			}
			c.History = append(c.History, history.Event{Seq: seq + 1, Type: history.ExecutionCompleted, Payload: []byte("1")})
			_, err := s.CompleteWorkflowTask(context.Background(), task.Lease, c)
			return err
		},
		"terminate": func(s *pg.Store, task store.WorkflowTask, _ int64) error {
			_, err := s.Terminate(context.Background(), task.Execution.InstanceID, "stop")
			return err
		},
	}
	stores := map[string]func(t *testing.T) *pg.Store{
		"fast": newStore, "pgx": newStoreWithoutFastPath,
		"hooked": func(t *testing.T) *pg.Store { // ends in a transaction of their own (inTx)
			t.Helper()
			s := newStore(t)
			s.SetHook(func(context.Context, pgx.Tx, store.EndInfo) error { return nil })
			return s
		},
	}
	for cn, tc := range cases {
		for sn, newS := range stores {
			for en, end := range ends {
				t.Run(cn+"/"+sn+"/"+en, func(t *testing.T) {
					t.Parallel()
					s := newS(t)
					task, seq := lockHolder(t, s, tc.holder, cn == "task")
					ctx := context.Background()
					x, err := s.Pool().Begin(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = x.Rollback(ctx) }()
					if _, err := x.Exec(ctx, tc.hold); err != nil {
						t.Fatal(err)
					}
					errc := make(chan error, 1)
					go func() { errc <- end(s, task, seq) }()
					waitForLockWait(t, s)
					for _, q := range tc.free {
						if _, err := x.Exec(ctx, q); err != nil {
							t.Fatalf("held by the end while it waits: %s: %v", q, err)
						}
					}
					if err := x.Rollback(ctx); err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-errc:
						if err != nil {
							t.Fatalf("end: %v", err)
						}
					case <-time.After(10 * time.Second):
						t.Fatal("the end did not finish")
					}
				})
			}
		}
	}
}

func pendingIDsOf(task store.WorkflowTask) []int64 {
	ids := make([]int64, len(task.Pending))
	for i, p := range task.Pending {
		ids[i] = p.ID
	}
	return ids
}

// lockHolder creates parent p with running child c; holder (p or c) takes
// lock k and, with act, schedules an activity (an unclaimed task). It
// returns holder's claimed task (a signal pending) and its history length.
func lockHolder(t *testing.T, s *pg.Store, holder string, act bool) (store.WorkflowTask, int64) {
	t.Helper()
	ctx := context.Background()
	claim := func(id string) store.WorkflowTask {
		t.Helper()
		tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
		if err != nil || len(tasks) != 1 || tasks[0].Execution.InstanceID != id {
			t.Fatalf("claim %s: %+v %v", id, tasks, err)
		}
		return tasks[0]
	}
	complete := func(task store.WorkflowTask, c *store.Completion) {
		t.Helper()
		c.Consumed = pendingIDsOf(task)
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "p", Workflow: "pw", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	complete(claim("p"), &store.Completion{
		History: []history.Event{
			{Seq: 1, Type: history.ExecutionStarted},
			{Seq: 2, Type: history.ChildScheduled, ScheduleID: 1, Ref: "c"},
		},
		Children: []store.NewExecution{{InstanceID: "c", Workflow: "cw", Version: 1, Queue: "q", ParentSched: 1}},
	})
	cseq := int64(1)
	complete(claim("c"), &store.Completion{History: []history.Event{{Seq: 1, Type: history.ExecutionStarted}}})
	seq := map[string]int64{"p": 2, "c": cseq}
	if err := s.Signal(ctx, holder, "go", nil); err != nil {
		t.Fatal(err)
	}
	task := claim(holder)
	c := &store.Completion{Locks: []store.LockRequest{{ScheduleID: 9, Key: "k", TTL: time.Minute}}}
	for _, p := range task.Pending {
		seq[holder]++
		ev := p.Event
		ev.Seq = seq[holder]
		c.History = append(c.History, ev)
	}
	seq[holder]++
	c.History = append(c.History, history.Event{Seq: seq[holder], Type: history.LockRequested, ScheduleID: 9, Name: "k"})
	if act {
		seq[holder]++
		c.History = append(c.History, history.Event{Seq: seq[holder], Type: history.ActivityScheduled, ScheduleID: 10, Name: "a"})
		c.Activities = []store.NewActivityTask{{ScheduleID: 10, Name: "a", Queue: "elsewhere"}}
	}
	complete(task, c)
	return claim(holder), seq[holder] // LockAcquired pending
}

// waitForLockWait waits until a session of s's database waits for a lock.
func waitForLockWait(t *testing.T, s *pg.Store) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; {
		var n int
		if err := s.Pool().QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the end never waited")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
