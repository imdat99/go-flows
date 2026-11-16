package worker

import (
	"sync/atomic"
	"testing"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// cached is an executor for the cache tests, and whether its workflow's
// coroutine was stopped (the executor closed).
type cached struct {
	*executor.Executor
	stopped *atomic.Bool
}

// waitingWorkflow schedules an activity with an input of actBytes, then
// waits for a signal; stopped is set when its coroutine is stopped.
func waitingWorkflow(actBytes int, stopped *atomic.Bool) executor.Workflow {
	return executor.Workflow{Name: "wf", Version: 1, Run: func(ctx coro.Context, _ []byte) ([]byte, error) {
		defer stopped.Store(true)
		st := executor.FromContext(ctx)
		if actBytes > 0 {
			st.ScheduleActivity(ctx, "A", "q", make([]byte, actBytes))
		}
		_, _ = st.SignalChannel("s").Receive(ctx)
		return nil, nil
	}}
}

func startEv(n int) history.Event {
	return history.Event{Type: history.ExecutionStarted, Name: "wf", Version: 1, Queue: "q", Payload: make([]byte, n)}
}

// waitingExecutor returns an executor of a workflow started with an input
// of n bytes, waiting for a signal (its coroutine stays live).
func waitingExecutor(t *testing.T, id int64, n int) cached {
	t.Helper()
	stopped := new(atomic.Bool)
	ex := executor.New(waitingWorkflow(0, stopped), id, executor.Options{})
	if _, err := ex.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEv(n)}}}); err != nil {
		t.Fatal(err)
	}
	return cached{ex, stopped}
}

// An executor's size grows with the payloads it holds, and is the same
// whether its history was run live or replayed (command payloads, such as
// an activity's input, included).
func TestExecutorSizeCountsPayloads(t *testing.T) {
	small, big := waitingExecutor(t, 1, 0), waitingExecutor(t, 2, 100_000)
	defer small.Close()
	defer big.Close()
	if small.Size() <= 0 {
		t.Fatalf("size of an empty executor %d, want > 0", small.Size())
	}
	if d := big.Size() - small.Size(); d < 100_000 {
		t.Fatalf("100 kB input adds %d bytes", d)
	}

	var stopped atomic.Bool
	wf := waitingWorkflow(50_000, &stopped)
	live := executor.New(wf, 3, executor.Options{})
	defer live.Close()
	c, err := live.Execute(&store.WorkflowTask{Pending: []store.PendingEvent{{ID: 1, Event: startEv(10)}}})
	if err != nil {
		t.Fatal(err)
	}
	replayed := executor.New(wf, 3, executor.Options{})
	defer replayed.Close()
	if err := replayed.Replay(c.History); err != nil {
		t.Fatal(err)
	}
	if _, err := replayed.Execute(&store.WorkflowTask{LastSeq: replayed.LastSeq()}); err != nil { // applies the held task
		t.Fatal(err)
	}
	if live.Size()-small.Size() < 50_000 {
		t.Fatalf("activity input not counted: live %d, empty %d", live.Size(), small.Size())
	}
	// The replayed one ran one more (empty) task: a TaskStarted, no bytes.
	if live.Size() != replayed.Size() {
		t.Fatalf("live size %d, replayed %d", live.Size(), replayed.Size())
	}
}

// Beyond the byte limit the least recently used executors are evicted and
// closed, even with room left by count.
func TestCacheEvictsLRUBeyondBytes(t *testing.T) {
	const payload = 100_000
	probe := waitingExecutor(t, 9, payload)
	one := probe.Size()
	probe.Close()
	c := newCache(100, 2*one+one/2) // room for two
	defer c.close()
	exs := []cached{waitingExecutor(t, 1, payload), waitingExecutor(t, 2, payload), waitingExecutor(t, 3, payload)}
	c.put(exs[0].Executor)
	c.put(exs[1].Executor)
	if got := c.take(1, exs[0].LastSeq()); got != exs[0].Executor { // 1 is now the most recently used
		t.Fatal("executor 1 not cached")
	}
	c.put(exs[0].Executor)
	c.put(exs[2].Executor) // over the limit: 2 is the least recently used
	if c.bytes > c.maxBytes {
		t.Fatalf("cache holds %d bytes, limit %d", c.bytes, c.maxBytes)
	}
	if !exs[1].stopped.Load() {
		t.Fatal("evicted executor not closed")
	}
	if got := c.take(2, exs[1].LastSeq()); got != nil {
		got.Close()
		t.Fatal("least recently used executor kept beyond the byte limit")
	}
	for _, id := range []int64{1, 3} {
		ex := exs[id-1]
		if ex.stopped.Load() {
			t.Fatalf("executor %d closed", id)
		}
		if got := c.take(id, ex.LastSeq()); got != ex.Executor {
			t.Fatalf("executor %d evicted", id)
		}
		ex.Close()
	}
	if c.bytes != 0 || c.lru.Len() != 0 {
		t.Fatalf("empty cache counts %d bytes, %d entries", c.bytes, c.lru.Len())
	}
}

// An executor larger than the whole limit is closed and not kept, and it
// evicts nothing; a stale executor of the same execution goes with it.
func TestCacheDropsAnExecutorLargerThanTheLimit(t *testing.T) {
	small := waitingExecutor(t, 1, 0)
	c := newCache(100, small.Size()+50_000)
	defer c.close()
	c.put(small.Executor)
	big := waitingExecutor(t, 2, 200_000)
	c.put(big.Executor)
	if !big.stopped.Load() {
		t.Fatal("executor larger than the limit not closed")
	}
	if got := c.take(2, big.LastSeq()); got != nil {
		t.Fatal("executor larger than the limit cached")
	}
	if small.stopped.Load() || c.lru.Len() != 1 || c.bytes != small.Size() {
		t.Fatalf("other executors evicted: closed %v, %d entries, %d bytes", small.stopped.Load(), c.lru.Len(), c.bytes)
	}

	// The same execution, cached small, then grown beyond the limit.
	grown := waitingExecutor(t, 1, 0)
	sig := history.Event{Type: history.SignalReceived, Name: "other", Payload: make([]byte, 100_000)}
	if _, err := grown.Execute(&store.WorkflowTask{LastSeq: grown.LastSeq(), Pending: []store.PendingEvent{{ID: 2, Event: sig}}}); err != nil {
		t.Fatal(err)
	}
	c.put(grown.Executor)
	if !grown.stopped.Load() || !small.stopped.Load() {
		t.Fatalf("closed: grown %v, stale %v", grown.stopped.Load(), small.stopped.Load())
	}
	if c.lru.Len() != 0 || c.bytes != 0 {
		t.Fatalf("cache counts %d entries, %d bytes", c.lru.Len(), c.bytes)
	}
}

// Replacing an executor with a newer one of the same execution closes the
// older one and keeps the byte count exact.
func TestCacheReplaceKeepsBytes(t *testing.T) {
	c := newCache(100, -1)
	defer c.close()
	a := waitingExecutor(t, 1, 10)
	c.put(a.Executor)
	b := waitingExecutor(t, 1, 5000)
	sig := history.Event{Type: history.SignalReceived, Name: "other", Payload: make([]byte, 100)}
	if _, err := b.Execute(&store.WorkflowTask{LastSeq: b.LastSeq(), Pending: []store.PendingEvent{{ID: 2, Event: sig}}}); err != nil { // further along
		t.Fatal(err)
	}
	c.put(b.Executor)
	if !a.stopped.Load() {
		t.Fatal("replaced executor not closed")
	}
	if c.bytes != b.Size() || c.lru.Len() != 1 {
		t.Fatalf("cache counts %d bytes in %d entries, holds %d", c.bytes, c.lru.Len(), b.Size())
	}
}

// A cache hit and the put after the task allocate only the list element,
// as before the byte bound (the hot path of every sticky task).
func TestCacheWarmCycleAllocations(t *testing.T) {
	c := newCache(1000, DefaultCacheBytes)
	defer c.close()
	ex := waitingExecutor(t, 1, 100)
	c.put(ex.Executor)
	if n := testing.AllocsPerRun(1000, func() { c.put(c.take(1, ex.LastSeq())) }); n > 1 {
		t.Fatalf("%v allocations per take and put, want at most 1 (the list element)", n)
	}
}
