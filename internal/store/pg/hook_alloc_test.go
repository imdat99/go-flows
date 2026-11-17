package pg_test

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
)

// endAllocs ends n fresh executions of s, each by its first task, and
// returns the integer average of the allocations of CompleteWorkflowTask
// (as testing.AllocsPerRun: fewer allocations than ends is 0). The first
// ends warm the connection up (prepared statements, buffers) and are not
// counted.
func endAllocs(t *testing.T, s *pg.Store, n int) uint64 {
	t.Helper()
	ctx := context.Background()
	const warm = 5
	var total uint64
	var before, after runtime.MemStats
	for i := range warm + n {
		id := "i" + strconv.Itoa(i)
		if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "wf", Version: 1, Queue: "q"}); err != nil {
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
		runtime.ReadMemStats(&before)
		_, err = s.CompleteWorkflowTask(ctx, tasks[0].Lease, c)
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		if i >= warm {
			total += after.Mallocs - before.Mallocs
		}
	}
	var status store.Status
	if err := s.Pool().QueryRow(ctx, `SELECT status FROM flows.execution WHERE instance_id = 'i0'`).Scan(&status); err != nil || status != store.StatusCompleted {
		t.Fatalf("status %q %v", status, err)
	}
	return total / uint64(n)
}

// T11.10: on Postgres, ending an execution with no end hook allocates
// nothing: the end takes the fast path, without the hook's transaction,
// identity read and closures. Under the race detector, which drops
// sync.Pool items at random (pooled buffers are allocated again), it is
// only checked to cost well under a hooked end.
func TestNoHookEndAllocatesNothing(t *testing.T) {
	const n = 50
	plain := endAllocs(t, newStore(t), n)
	hookedStore := newStore(t)
	hookedStore.SetHook(func(context.Context, pgx.Tx, store.EndInfo) error { return nil })
	hooked := endAllocs(t, hookedStore, n)
	t.Logf("end: %d allocations without a hook, %d with one", plain, hooked)
	if !raceEnabled && plain != 0 {
		t.Errorf("end without a hook: %d allocations", plain)
	}
	if plain*2 >= hooked {
		t.Errorf("end without a hook: %d allocations, with a no-op hook: %d", plain, hooked)
	}
}
