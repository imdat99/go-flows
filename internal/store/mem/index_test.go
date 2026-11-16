package mem

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

const ttl = time.Minute

// claim claims the one workflow task of queue (each test execution has a
// queue of its own), failing the test when there is none.
func claim(t *testing.T, s *Store, queue string) store.WorkflowTask {
	t.Helper()
	ts, err := s.ClaimWorkflowTasks(context.Background(), "o", []string{queue}, 1, ttl)
	if err != nil || len(ts) != 1 {
		t.Fatalf("claim %s: %d tasks, %v", queue, len(ts), err)
	}
	return ts[0]
}

// complete completes task with c, consuming every pending event it carries.
func complete(t *testing.T, s *Store, task store.WorkflowTask, c store.Completion) {
	t.Helper()
	for _, p := range task.Pending {
		c.Consumed = append(c.Consumed, p.ID)
	}
	if _, err := s.CompleteWorkflowTask(context.Background(), task.Lease, &c); err != nil {
		t.Fatalf("complete %s: %v", task.Execution.InstanceID, err)
	}
}

func create(t *testing.T, s *Store, id, queue string) {
	t.Helper()
	if _, _, err := s.CreateExecution(context.Background(), store.NewExecution{InstanceID: id, Workflow: "w", Queue: queue}); err != nil {
		t.Fatal(err)
	}
}

// TestIndexedLookupsVisitMatchingSet: ending a parent visits its children
// only, and Wake the claimable executions of its queues only, however many
// other executions the store holds (T10.8).
func TestIndexedLookupsVisitMatchingSet(t *testing.T) {
	s := New(&ManualClock{})
	for i := range 500 {
		create(t, s, fmt.Sprintf("bulk-%d", i), "bulk")
	}
	create(t, s, "parent", "p")
	complete(t, s, claim(t, s, "p"), store.Completion{Children: []store.NewExecution{
		{InstanceID: "c1", Workflow: "w", Queue: "c", ParentSched: 1},
		{InstanceID: "c2", Workflow: "w", Queue: "c", ParentSched: 2},
		{InstanceID: "c3", Workflow: "w", Queue: "c", ParentSched: 3},
	}})

	if err := s.Signal(context.Background(), "parent", "go", nil); err != nil {
		t.Fatal(err)
	}
	s.visited = 0
	complete(t, s, claim(t, s, "p"), store.Completion{Status: store.StatusFailed, Err: "boom"})
	if s.visited > 3 {
		t.Errorf("ending a parent of 3 children visited %d executions", s.visited)
	}

	s.visited = 0
	if ready, _, _ := s.Wake("o", []string{"c"}); !ready {
		t.Error("Wake: the children are not ready")
	}
	if s.visited > 3 {
		t.Errorf("Wake on a queue of 3 executions visited %d", s.visited)
	}
}

// checkIndexes compares the indexes with a scan of every execution, and
// Wake with the scan it replaced (T10.8).
func checkIndexes(t *testing.T, s *Store, step string) {
	t.Helper()
	checkRunnable(t, s, step)
	checkChildren(t, s, step)
	for _, qs := range [][]string{{"qa"}, {"qb"}, {"qk"}, {"qa", "qk", "qa"}, {"none"}} {
		ready, next, ok := s.Wake("o", qs)
		bReady, bNext, bOK := wakeScan(s, "o", qs)
		if ready != bReady || !next.Equal(bNext) || ok != bOK {
			t.Errorf("%s: Wake(%v) = %v %v %v, the full scan gives %v %v %v", step, qs, ready, next, ok, bReady, bNext, bOK)
		}
	}
}

func checkRunnable(t *testing.T, s *Store, step string) {
	t.Helper()
	want := 0
	for _, e := range s.executions {
		if claimable(e.Status) {
			want++
			if s.runnable[e.Queue][e.ID] != e {
				t.Errorf("%s: claimable %s (%s) not in the runnable index", step, e.InstanceID, e.Status)
			}
		}
	}
	n := 0
	for q, m := range s.runnable {
		for id, e := range m {
			n++
			if s.executions[id] != e || e.Queue != q || !claimable(e.Status) {
				t.Errorf("%s: stale runnable entry %d (%s)", step, id, e.Status)
			}
		}
	}
	if n != want {
		t.Errorf("%s: runnable index holds %d executions, want %d", step, n, want)
	}
}

func checkChildren(t *testing.T, s *Store, step string) {
	t.Helper()
	want := 0
	for _, e := range s.executions {
		if e.ParentID != 0 {
			want++
			if n := countOf(s.children[e.ParentID], e); n != 1 {
				t.Errorf("%s: %s is %d times in its parent's children", step, e.InstanceID, n)
			}
		}
	}
	n := 0
	for p, cs := range s.children {
		if len(cs) == 0 {
			t.Errorf("%s: empty children entry for %d", step, p)
		}
		for _, c := range cs {
			n++
			if s.executions[c.ID] != c || c.ParentID != p {
				t.Errorf("%s: stale child entry %d of %d", step, c.ID, p)
			}
		}
	}
	if n != want {
		t.Errorf("%s: children index holds %d executions, want %d", step, n, want)
	}
}

func countOf(cs []*execution, e *execution) int {
	n := 0
	for _, c := range cs {
		if c == e {
			n++
		}
	}
	return n
}

// wakeScan is Wake as it was before T10.8: a scan of every execution.
func wakeScan(s *Store, owner string, queues []string) (ready bool, next time.Time, ok bool) {
	now := s.clock.Now()
	consider := func(at time.Time) {
		if !at.After(now) {
			ready = true
			return
		}
		if !ok || at.Before(next) {
			next, ok = at, true
		}
	}
	for _, e := range s.executions {
		if !claimable(e.Status) || len(e.pending) == 0 && !e.forceReady || !contains(queues, e.Queue) {
			continue
		}
		earliest := now
		if !e.forceReady {
			earliest = e.pending[0].visibleAt
			for _, p := range e.pending[1:] {
				if p.visibleAt.Before(earliest) {
					earliest = p.visibleAt
				}
			}
		}
		at := latest(earliest, e.lockedUntil, e.notBefore)
		if e.stickyOwner != "" && e.stickyOwner != owner {
			at = latest(at, e.stickyUntil)
		}
		consider(at)
	}
	for _, t := range s.tasks {
		if contains(queues, t.Queue) {
			consider(latest(t.visibleAt, t.lockedUntil))
		}
	}
	return ready, next, ok
}

// TestIndexesFollowEveryChange runs executions through every change of
// status, parent and existence, checking the indexes after each (T10.8).
func TestIndexesFollowEveryChange(t *testing.T) {
	ctx := context.Background()
	clock := &ManualClock{}
	s := New(clock)
	checkIndexes(t, s, "empty")

	create(t, s, "r1", "qa")
	create(t, s, "r2", "qb")
	create(t, s, "r3", "qa")
	checkIndexes(t, s, "create")

	complete(t, s, claim(t, s, "qa"), store.Completion{
		Children: []store.NewExecution{
			{InstanceID: "k1", Workflow: "w", Queue: "qk", ParentSched: 1},
			{InstanceID: "k2", Workflow: "w", Queue: "qk", ParentSched: 2},
		},
		Timers: []history.Event{{ScheduleID: 9, Name: "t", At: clock.Now().Add(time.Hour)}},
	})
	checkIndexes(t, s, "children")

	// r3, claimed and kept, ends; k1 completes, k2 continues as new.
	complete(t, s, claim(t, s, "qa"), store.Completion{Status: store.StatusCompleted})
	checkIndexes(t, s, "r3 completed")
	k1 := claim(t, s, "qk")
	if k1.Execution.InstanceID != "k1" {
		t.Fatalf("claimed %s, want k1", k1.Execution.InstanceID)
	}
	complete(t, s, k1, store.Completion{Status: store.StatusCompleted})
	checkIndexes(t, s, "k1 completed")
	complete(t, s, claim(t, s, "qk"), store.Completion{
		Status:        store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "w", Queue: "qk"},
	})
	checkIndexes(t, s, "k2 continued")

	// r1 compensates k1: completed -> compensating -> completed.
	complete(t, s, claim(t, s, "qa"), store.Completion{CompensateChildren: []int32{1, 2}})
	checkIndexes(t, s, "k1 reopened")
	if k := s.executions[s.latest["k1"]]; k.Status != store.StatusCompensating {
		t.Fatalf("k1 is %s, want compensating", k.Status)
	}
	complete(t, s, claim(t, s, "qk"), store.Completion{Status: store.StatusCompleted})
	checkIndexes(t, s, "k1 compensated")

	// r2: parked, reopened, parked for its compensation, reopened, ends.
	complete(t, s, claim(t, s, "qb"), store.Completion{Status: store.StatusNeedsAttention, Err: "nd", ErrKinds: store.KindNonDeterministic})
	checkIndexes(t, s, "r2 parked")
	if err := s.ReopenParked(ctx, "r2"); err != nil {
		t.Fatal(err)
	}
	checkIndexes(t, s, "r2 reopened")
	complete(t, s, claim(t, s, "qb"), store.Completion{Status: store.StatusNeedsAttention, Err: "c", ErrKinds: store.KindCompensationFailed})
	checkIndexes(t, s, "r2 parked again")
	if err := s.ReopenCompensation(ctx, "r2"); err != nil {
		t.Fatal(err)
	}
	checkIndexes(t, s, "r2 compensating")
	complete(t, s, claim(t, s, "qb"), store.Completion{Status: store.StatusCompensated})
	checkIndexes(t, s, "r2 compensated")

	// Terminating r1 asks the next run of k2 (still its child) to cancel.
	if _, err := s.Terminate(ctx, "r1", "stop"); err != nil {
		t.Fatal(err)
	}
	checkIndexes(t, s, "r1 terminated")
	k2 := claim(t, s, "qk")
	if !hasEvent(k2, history.ExecutionCancelRequested) {
		t.Fatal("the next run of k2 got no cancel request")
	}
	complete(t, s, k2, store.Completion{Status: store.StatusCanceled})
	checkIndexes(t, s, "k2 canceled")

	// A running execution stays; the ended ones go, parents first.
	create(t, s, "r4", "qa")
	clock.Advance(time.Hour)
	for i := 0; ; i++ {
		n, err := s.PurgeEnded(ctx, time.Minute, 1)
		if err != nil {
			t.Fatal(err)
		}
		checkIndexes(t, s, fmt.Sprintf("purge %d", i))
		if n == 0 {
			break
		}
	}
	if len(s.executions) != 1 || len(s.children) != 0 {
		t.Errorf("after purging: %d executions, %d children entries", len(s.executions), len(s.children))
	}
}

func hasEvent(task store.WorkflowTask, typ history.Type) bool {
	for _, p := range task.Pending {
		if p.Event.Type == typ {
			return true
		}
	}
	return false
}

func newExec(id, queue string) store.NewExecution {
	return store.NewExecution{InstanceID: id, Workflow: "w", Queue: queue}
}

// TestClaimRepeatedQueue: a queue listed twice claims its executions once.
func TestClaimRepeatedQueue(t *testing.T) {
	s := New(&ManualClock{})
	create(t, s, "a", "q")
	ts, err := s.ClaimWorkflowTasks(context.Background(), "o", []string{"q", "q"}, 10, ttl)
	if err != nil || len(ts) != 1 {
		t.Fatalf("claimed %d tasks, %v; want 1", len(ts), err)
	}
}

// A queue's index goes once the queue has no claimable execution: a map
// keeps its peak capacity, and ranging over it after a burst would cost
// as much as the scan the index replaced.
func TestRunnableQueueIndexShrinks(t *testing.T) {
	s := New(&ManualClock{})
	ctx := context.Background()
	for i := range 3 {
		create(t, s, fmt.Sprintf("burst-%d", i), "burst")
	}
	for i := range 3 {
		if _, err := s.Terminate(ctx, fmt.Sprintf("burst-%d", i), "done"); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	m, ok := s.runnable["burst"]
	s.mu.Unlock()
	if ok {
		t.Fatalf("index of an emptied queue kept (%d entries)", len(m))
	}
	create(t, s, "burst-again", "burst")
	claim(t, s, "burst")
}
