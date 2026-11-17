package flows

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

// idSeen is what one run of idWF's body saw.
type idSeen struct {
	n         int
	replaying bool
	instance  string
	execution int64
}

var (
	idMu   sync.Mutex
	idRuns []idSeen

	// idWF records its ids before and after a timer (a second task: with
	// no cache it replays the first), then continues as new once.
	idWF = DefineWorkflow("test.ids", 1, func(c Ctx, n int) (int64, error) {
		see := func() {
			idMu.Lock()
			defer idMu.Unlock()
			idRuns = append(idRuns, idSeen{n, Replaying(c), InstanceID(c), ExecutionID(c)})
		}
		see()
		if err := Sleep(c, time.Millisecond); err != nil {
			return 0, err
		}
		see()
		if n == 0 {
			return 0, ContinueAsNew(c, 1)
		}
		return ExecutionID(c), nil
	})
)

// T11.9: InstanceID and ExecutionID give the same values live and on
// replay; continue-as-new keeps the instance id and gives a new execution
// id. On both stores, without a cache (the second task replays).
func TestInstanceAndExecutionID(t *testing.T) {
	t.Run("mem", func(t *testing.T) {
		testInstanceAndExecutionID(t, startedEngine(t, mem.New(nil), Options{InstanceID: "ids-engine", CacheSize: -1}))
	})
	t.Run("pg", func(t *testing.T) {
		pool := testdb.New(t)
		if err := Migrate(bg, pool); err != nil {
			t.Fatal(err)
		}
		testInstanceAndExecutionID(t, startedEngine(t, pg.New(pool), Options{InstanceID: "ids-engine", CacheSize: -1}))
	})
}

func testInstanceAndExecutionID(t *testing.T, e *Engine) {
	t.Helper()
	idMu.Lock()
	idRuns = nil
	idMu.Unlock()
	last, err := idWF.Run(waitCtx(t), e, "ids-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	idMu.Lock()
	defer idMu.Unlock()
	var first int64
	for _, n := range []int{0, 1} {
		var runs []idSeen
		for _, r := range idRuns {
			if r.n == n {
				runs = append(runs, r)
			}
		}
		// Live first task; replay of it and live second task.
		if len(runs) != 3 || runs[0].replaying || !runs[1].replaying || runs[2].replaying {
			t.Fatalf("execution %d: runs %+v", n, runs)
		}
		for _, r := range runs {
			if r.instance != "ids-1" || r.execution != runs[0].execution || r.execution == 0 {
				t.Fatalf("execution %d: runs %+v", n, runs)
			}
		}
		if n == 0 {
			first = runs[0].execution
		} else if runs[0].execution == first || runs[0].execution != last {
			t.Fatalf("execution ids: first %d, second %+v, returned %d", first, runs, last)
		}
	}
}

// T11.9: the accessors allocate nothing.
func TestInstanceAndExecutionIDAllocateNothing(t *testing.T) {
	var ids, exs float64
	var gotID string
	var gotEx int64
	wf := executor.Workflow{Name: "test.ids-alloc", Version: 1, Run: func(cc coro.Context, _ []byte) ([]byte, error) {
		c := Ctx{cc}
		gotID, gotEx = InstanceID(c), ExecutionID(c)
		ids = testing.AllocsPerRun(100, func() { gotID = InstanceID(c) })
		exs = testing.AllocsPerRun(100, func() { gotEx = ExecutionID(c) })
		return nil, nil
	}}
	ex := executor.New(wf, 7, executor.Options{WorkflowInstanceID: "alloc-1"})
	defer ex.Close()
	task := &store.WorkflowTask{Now: time.Unix(1000, 0), Pending: []store.PendingEvent{{ID: 1, Event: history.Event{
		Type: history.ExecutionStarted, Name: wf.Name, Version: 1, Queue: "q",
	}}}}
	if _, err := ex.Execute(task); err != nil {
		t.Fatal(err)
	}
	if gotID != "alloc-1" || gotEx != 7 {
		t.Fatalf("ids %q %d", gotID, gotEx)
	}
	if ids != 0 || exs != 0 {
		t.Fatalf("InstanceID: %v allocations, ExecutionID: %v", ids, exs)
	}
}

// T11.10: Terminate after continue-as-new to another workflow and version
// tells the observer the workflow and version of the execution it ended
// (the last one), on both stores.
// The public API continues as new into the same definition only
// (ContinueAsNew), so the completion is injected at the store level: the
// observer must read the latest execution, whatever produced it.
func TestTerminateAfterContinueAsNewObservesLastExecution(t *testing.T) {
	t.Run("mem", func(t *testing.T) { testTerminateAfterContinueAsNew(t, mem.New(nil)) })
	t.Run("pg", func(t *testing.T) {
		pool := testdb.New(t)
		if err := Migrate(bg, pool); err != nil {
			t.Fatal(err)
		}
		testTerminateAfterContinueAsNew(t, pg.New(pool))
	})
}

func testTerminateAfterContinueAsNew(t *testing.T, st store.Store) {
	t.Helper()
	r := &recorder{}
	// Not started: the store runs the executions; the engine terminates.
	e, err := newEngine(st, Options{InstanceID: "term-can", Observer: obs{r}})
	if err != nil {
		t.Fatal(err)
	}
	const id = "term-can"
	if _, _, err := st.CreateExecution(bg, store.NewExecution{InstanceID: id, Workflow: "test.can-old", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	tasks, err := st.ClaimWorkflowTasks(bg, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim %v %v", tasks, err)
	}
	task := tasks[0]
	consumed := make([]int64, 0, len(task.Pending))
	for _, p := range task.Pending {
		consumed = append(consumed, p.ID)
	}
	if _, err := st.CompleteWorkflowTask(bg, task.Lease, &store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ExecutionContinuedAsNew}},
		Consumed: consumed, Status: store.StatusContinued,
		ContinueAsNew: &store.NewExecution{Workflow: "test.can-new", Version: 3, Queue: "q"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Terminate(bg, id, "operator"); err != nil {
		t.Fatal(err)
	}
	runs := r.runsOf(func(r *recorder) []RunInfo { return r.ended }, id)
	want := RunInfo{InstanceID: id, Workflow: "test.can-new", Version: 3, Status: StatusTerminated, Error: "operator"}
	if !slices.Equal(runs, []RunInfo{want}) {
		t.Fatalf("observed %+v, want %+v", runs, want)
	}
	x, err := st.GetExecution(bg, id)
	if err != nil || x.Status != StatusTerminated || x.Workflow != "test.can-new" {
		t.Fatalf("latest execution %+v %v", x, err)
	}
}

// T11.10: on Postgres, the engine's observer sees every end: a workflow's
// end committed by a worker (with and without a run hook: the store's
// pgx and fast paths) and a termination.
func TestPostgresObserverRunEnded(t *testing.T) {
	t.Run("observer", func(t *testing.T) { testPostgresObserver(t, false) })
	t.Run("observer+hook", func(t *testing.T) { testPostgresObserver(t, true) })
}

func testPostgresObserver(t *testing.T, hooked bool) {
	t.Helper()
	pool := testdb.New(t)
	if err := Migrate(bg, pool); err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	opts := Options{InstanceID: "pg-obs", Observer: obs{r}}
	if hooked {
		opts.Hook = r
	}
	e := startedEngine(t, pg.New(pool), opts)
	comps.reset("hook-saga")
	if _, err := sagaWF.Run(waitCtx(t), e, "hook-saga", []string{"a"}); err == nil {
		t.Fatal("saga succeeded")
	}
	if v, err := countWF.Run(waitCtx(t), e, "obs-count", 3); err != nil || v != 5 {
		t.Fatalf("count %d %v", v, err)
	}
	h, err := waitForeverWF.Start(bg, e, "hook-term", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { st, _ := h.Status(bg); return st == StatusRunning })
	if err := e.Terminate(bg, "hook-term", "operator"); err != nil {
		t.Fatal(err)
	}
	ended := func(r *recorder) []RunInfo { return r.ended }
	// The observer follows the commit: it may lag the waiter.
	waitFor(t, func() bool { return len(r.runsOf(ended, "hook-saga")) > 0 && len(r.runsOf(ended, "obs-count")) > 0 })
	checkEnds(t, "observer", r, ended)
	if hooked {
		checkEnds(t, "hook", r, func(r *recorder) []RunInfo { return r.hooked })
	}
	r.checkAttempts(t)
}

// checkEnds expects one end of each of testPostgresObserver's instances in
// list: continue-as-new is not an end, so the count chain ends once.
func checkEnds(t *testing.T, name string, r *recorder, list func(*recorder) []RunInfo) {
	t.Helper()
	saga := r.runsOf(list, "hook-saga")
	if len(saga) != 1 || saga[0].Error == "" {
		t.Fatalf("%s: saga %+v", name, saga)
	}
	saga[0].Error = ""
	want := RunInfo{InstanceID: "hook-saga", Workflow: "comp.saga", Version: 1, Status: StatusCompensated, ErrorKinds: "test_comp_no_stock"}
	if saga[0] != want {
		t.Fatalf("%s: saga %+v, want %+v", name, saga[0], want)
	}
	for _, want := range []RunInfo{
		{InstanceID: "obs-count", Workflow: "test.count", Version: 1, Status: StatusCompleted},
		{InstanceID: "hook-term", Workflow: "test.forever", Version: 1, Status: StatusTerminated, Error: "operator"},
	} {
		if got := r.runsOf(list, want.InstanceID); !slices.Equal(got, []RunInfo{want}) {
			t.Fatalf("%s: %+v, want %+v", name, got, want)
		}
	}
}
