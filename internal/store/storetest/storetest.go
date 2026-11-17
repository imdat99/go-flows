// Package storetest is the conformance suite of store.Store. Each
// implementation runs it from its own tests:
//
//	storetest.Run(t, storetest.Harness{New: ..., Advance: ...})
//
// The shape is inspired by go-workflows backend/test (MIT, see NOTICE). The
// cases are the guarantees of the store package, including regression tests
// for the go-workflows backend bugs (GW-1 to GW-6).
package storetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Harness adapts an implementation to the suite.
type Harness struct {
	// New returns an empty store, released when the test ends.
	New func(t *testing.T) store.Store
	// Advance moves the store clock forward by at least d: the memory store
	// advances its manual clock, Postgres sleeps.
	Advance func(t *testing.T, s store.Store, d time.Duration)
	// TTL is the lease duration the suite uses; it must be long enough for a
	// test step to complete within it on the implementation, and Advance(TTL)
	// must be affordable. Zero means 1s.
	TTL time.Duration
	// SetHook installs an end hook on s (nil: the hook cases are skipped).
	SetHook func(s store.Store, h func(ctx context.Context, e store.EndInfo) error)
}

const (
	queue = "default"
	owner = "owner-a"
	other = "owner-b"
)

var ctx = context.Background()

// Run runs every conformance case.
func Run(t *testing.T, h Harness) {
	if h.TTL == 0 {
		h.TTL = time.Second
	}
	cases := []struct {
		name string
		fn   func(t *testing.T, h Harness)
	}{
		{"CreateIsIdempotent", testCreateIsIdempotent},
		{"GW1_ConcurrentCreateMakesOneExecution", testGW1ConcurrentCreate},
		{"ClaimDeliversStartAndLocks", testClaimDeliversStartAndLocks},
		{"ClaimFiltersQueuesAndLimits", testClaimFiltersQueuesAndLimits},
		{"ClaimOrdersByReadiness", testClaimOrdersByReadiness},
		{"GW2_StaleLeaseOfSameOwnerIsFenced", testGW2StaleLeaseSameOwner},
		{"CompleteAppendsHistoryAndConsumes", testCompleteAppendsHistory},
		{"CompleteIsAtomicOnInvalidHistory", testCompleteIsAtomic},
		{"TerminalStatusEndsExecution", testTerminalStatus},
		{"KeepHoldsTheLease", testKeepHoldsLease},
		{"ReleaseWithDelay", testReleaseWithDelay},
		{"StickyToOwner", testSticky},
		{"TimersBecomeVisibleAtTheirTime", testTimers},
		{"ActivityRoundTrip", testActivityRoundTrip},
		{"GW2_ActivityLeaseIsFenced", testActivityLeaseFenced},
		{"ActivityRetryWithDelay", testActivityRetry},
		{"EagerActivityClaim", testEagerActivityClaim},
		{"ActivityResultForEndedExecutionIsDropped", testActivityResultDropped},
		{"SignalsAreFIFOAndChecked", testSignals},
		{"CancelRequest", testCancelRequest},
		{"GW3_ContinueAsNewCarriesSignals", testContinueAsNewCarriesSignals},
		{"GW3_SignalRacingCompletionIsNotLost", testSignalRacingCompletion},
		{"ChildrenAndParentNotification", testChildren},
		{"ChildOutcomeKinds", testChildOutcomeKinds},
		{"GW3_TerminalCompletionWithUndeliveredSignalIsRejected", testTerminalWithPendingSignal},
		{"GW3_ContinueAsNewCarriesCancelRequests", testContinueAsNewCarriesCancel},
		{"PendingOrderMixesTimersAndSignals", testPendingOrder},
		{"PartialConsume", testPartialConsume},
		{"EagerLoopKeepResume", testEagerLoop},
		{"ReleaseClearsStickiness", testReleaseClearsSticky},
		{"ExpiredLeaseNotRegrantedIsStillValid", testExpiredLeaseStillValid},
		{"NeedsAttentionIsParkedCompensatingRuns", testParkedStatuses},
		{"InvalidCompletions", testInvalidCompletions},
		{"ActivityQueuesAndLimit", testActivityQueuesAndLimit},
		{"DefensiveCopies", testDefensiveCopies},
		{"ClaimsWithRepeatedQueues", testClaimsWithRepeatedQueues},
		{"DueClaims", testDueClaims},
		{"DueSkipsManyKeptLeases", testDueSkipsManyKeptLeases},
		{"DueSkipsTimersOfStickyExecutions", testDueSkipsStickyTimers},
		{"ChildrenWithTheEnd", testChildrenWithTheEnd},
		{"NoteStuck", testNoteStuck},
		{"TimesComeFromTheStoreClock", testStoreClock},
		{"GW3_SignalsRacingTerminalCompletion", testSignalsRacingTerminal},
		{"ParkedExecutionCannotKeepItsLease", testParkedCannotKeep},
		{"RenewLeases", testRenewLeases},
		{"GW4_PurgeEnded", testPurgeEnded},
		{"GW4_PurgeKeepsActiveExecutions", testPurgeKeepsActiveExecutions},
		{"GW4_PurgeKeepsDescendantsOfActiveExecutions", testPurgeKeepsDescendantsOfActiveExecutions},
		{"KeptLeaseHolderDies", testKeptLeaseHolderDies},
		{"KeptLeaseExpiresWithoutEvents", testKeptLeaseExpiresWithoutEvents},
		{"GW3_TerminalWithNilConsumed", testTerminalWithNilConsumed},
		{"WatchHints", testWatch},
		{"Terminate", testTerminate},
		{"CompensationTask", testCompensationTask},
		{"ActivityResultTypes", testActivityResultTypes},
		{"ReopenCompensation", testReopenCompensation},
		{"ReopenParked", testReopenParked},
		{"CompensateChildren", testCompensateChildren},
		{"Locks", testLocks},
		{"LockRenewal", testLockRenewal},
		{"LockValidation", testLockValidation},
		{"LockHeldDuringActivity", testLockHeldDuringActivity},
		{"LockKeptWhileActivityWaits", testLockKeptWhileActivityWaits},
		{"LockKeptWhileEventsWait", testLockKeptWhileEventsWait},
		{"LockAnswerInCompletion", testLockAnswerInCompletion},
		{"LockWaitersFIFO", testLockWaitersFIFO},
		{"LockWaiterExpires", testLockWaiterExpires},
		{"LockWaiterEdges", testLockWaiterEdges},
		{"LockWaiterLapsed", testLockWaiterLapsed},
		{"LockWaiterLastAttempt", testLockWaiterLastAttempt},
		{"LockWaiterHeadTerminated", testLockWaiterHeadTerminated},
		{"LockWaiterRace", testLockWaiterRace},
		{"LockWaiterStaleWoken", testLockWaiterStaleWoken},
		{"LockWaiterTerminatedHolder", testLockWaiterTerminatedHolder},
		{"LockWaiterParkedSkipped", testLockWaiterParkedSkipped},
		{"ResumeParity", testResumeParity},
		{"ResumeCompensating", testResumeCompensating},
		{"ResumeParked", testResumeParked},
		{"ResumeWithEager", testResumeWithEager},
		{"LockLostBlocksEnd", testLockLostBlocksEnd},
		{"LockRace", testLockRace},
		{"LockNotTakenFromRunningHolder", testLockNotTakenFromRunningHolder},
		{"EndHook", testEndHook},
		{"EndHookCalls", testEndHookCalls},
		{"EndHookPendingEvents", testEndHookPendingEvents},
		{"LockHolderParked", testLockHolderParked},
		{"LockHolderParkedTerminated", testLockHolderParkedTerminated},
		{"Unstick", testUnstick},
		{"EndCancelsChildren", testEndCancelsChildren},
		{"GW3_ContinueAsNewCarriesUnreadSignals", testContinueAsNewCarriesUnread},
		{"ContinueAsNewLeavesChildren", testContinueAsNewLeavesChildren},
		{"PurgeKeepsChildrenOfActiveParents", testPurgeKeepsChildrenOfActiveParents},
		{"UncertainAcrossAttempts", testUncertainAcrossAttempts},
		{"CompDataAcrossAttempts", testCompDataAcrossAttempts},
		{"GW13_ActivityCancelReachesItsWorker", testActivityCancel},
		{"GW13_CancelBeforeRetry", testCancelBeforeRetry},
		{"TraceContext", testTraceContext},
		{"TraceOnCompensation", testTraceOnCompensation},
		{"ActivityLostAttemptsExhaust", testActivityLostAttemptsExhaust},
		{"ActivityExhaustEdges", testActivityExhaustEdges},
		{"ReleaseActivityTask", testReleaseActivityTask},
		{"WorkflowClaimsCount", testWorkflowClaimsCount},
		{"PurgedIDsKept", testPurgedIDsKept},
		{"PurgedIDsOffByDefault", testPurgedIDsOffByDefault},
		{"PurgedIDOfChain", testPurgedIDOfChain},
		{"PurgedChildID", testPurgedChildID},
		{"PurgeRacesCreate", testPurgeRacesCreate},
		{"SignalWorkflow", testSignalWorkflow},
		{"SignalWorkflowContinueAsNew", testSignalWorkflowContinueAsNew},
		{"SignalWorkflowAtomic", testSignalWorkflowAtomic},
		{"SignalWorkflowMutual", testSignalWorkflowMutual},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, h)
		})
	}
}

func newExec(id string) store.NewExecution {
	return store.NewExecution{InstanceID: id, Workflow: "wf", Version: 1, Queue: queue, Input: []byte(`{"n":1}`)}
}

func mustCreate(t *testing.T, s store.Store, id string) store.Execution {
	t.Helper()
	e, created, err := s.CreateExecution(ctx, newExec(id))
	if err != nil || !created {
		t.Fatalf("create %s: created=%v err=%v", id, created, err)
	}
	return e
}

func claim(t *testing.T, s store.Store, who string, ttl time.Duration) []store.WorkflowTask {
	t.Helper()
	tasks, err := s.ClaimWorkflowTasks(ctx, who, []string{queue}, 10, ttl)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return tasks
}

func claimOne(t *testing.T, s store.Store, who string, ttl time.Duration) store.WorkflowTask {
	t.Helper()
	tasks := claim(t, s, who, ttl)
	if len(tasks) != 1 {
		for _, x := range tasks {
			t.Logf("claimed %s: %+v", x.Execution.InstanceID, x.Pending)
		}
		t.Fatalf("claimed %d tasks, want 1", len(tasks))
	}
	return tasks[0]
}

func pendingIDs(task store.WorkflowTask) []int64 {
	ids := make([]int64, len(task.Pending))
	for i, p := range task.Pending {
		ids[i] = p.ID
	}
	return ids
}

func pendingTypes(task store.WorkflowTask) []history.Type {
	out := make([]history.Type, len(task.Pending))
	for i, p := range task.Pending {
		out[i] = p.Event.Type
	}
	return out
}

// started completes the first task of a fresh execution: history gets
// ExecutionStarted, the start event is consumed, the lease released.
func started(t *testing.T, s store.Store, task store.WorkflowTask, extra *store.Completion) {
	t.Helper()
	c := store.Completion{}
	if extra != nil {
		c = *extra
	}
	c.History = append([]history.Event{task.Pending[0].Event}, c.History...)
	for i := range c.History {
		c.History[i].Seq = task.LastSeq + int64(i) + 1
	}
	c.Consumed = append(c.Consumed, pendingIDs(task)...)
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func testCreateIsIdempotent(t *testing.T, h Harness) {
	s := h.New(t)
	e := mustCreate(t, s, "i1")
	if e.ID == 0 || e.Status != store.StatusRunning || e.InstanceID != "i1" || string(e.Input) != `{"n":1}` {
		t.Fatalf("created %+v", e)
	}
	again, created, err := s.CreateExecution(ctx, newExec("i1"))
	if err != nil || created || again.ID != e.ID {
		t.Fatalf("second create: %+v created=%v err=%v", again, created, err)
	}
	ne := newExec("i1")
	ne.Workflow = "other"
	if _, _, err := s.CreateExecution(ctx, ne); !errors.Is(err, store.ErrInstanceConflict) {
		t.Fatalf("other workflow: %v", err)
	}
	got, err := s.GetExecution(ctx, "i1")
	if err != nil || got.ID != e.ID {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := s.GetExecution(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
}

// GW-1: concurrent creates of one instance id produce one execution.
func testGW1ConcurrentCreate(t *testing.T, h Harness) {
	s := h.New(t)
	const n = 8
	var wg sync.WaitGroup
	ids := make([]int64, n)
	createdCount := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			e, created, err := s.CreateExecution(ctx, newExec("same"))
			ids[i], createdCount[i], errs[i] = e.ID, created, err
		})
	}
	wg.Wait()
	created := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("create %d: %v", i, errs[i])
		}
		if createdCount[i] {
			created++
		}
		if ids[i] != ids[0] {
			t.Fatalf("different executions returned: %v", ids)
		}
	}
	if created != 1 {
		t.Fatalf("%d creates reported created, want 1", created)
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 1 || len(got[0].Pending) != 1 {
		t.Fatalf("want one claimable execution with one start event, got %d", len(got))
	}
}

func testClaimDeliversStartAndLocks(t *testing.T, h Harness) {
	s := h.New(t)
	e := mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if task.Lease.ID != e.ID || task.Lease.Owner != owner || task.Lease.Version < 1 || task.LastSeq != 0 {
		t.Fatalf("task %+v", task)
	}
	if got := pendingTypes(task); !slices.Equal(got, []history.Type{history.ExecutionStarted}) {
		t.Fatalf("pending %v", got)
	}
	start := task.Pending[0].Event
	if start.Name != "wf" || start.Version != 1 || start.Queue != queue || string(start.Payload) != `{"n":1}` {
		t.Fatalf("start event %+v", start)
	}
	if task.Now.IsZero() {
		t.Fatal("task has no store time")
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("claimed an execution with a live lease")
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("re-granted a live lease to the same owner")
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	again := claimOne(t, s, other, h.TTL)
	if again.Lease.Version <= task.Lease.Version {
		t.Fatalf("lease version did not grow: %d -> %d", task.Lease.Version, again.Lease.Version)
	}
}

func testClaimFiltersQueuesAndLimits(t *testing.T, h Harness) {
	s := h.New(t)
	for i := range 3 {
		mustCreate(t, s, fmt.Sprint("q", i))
	}
	ne := newExec("elsewhere")
	ne.Queue = "other"
	if _, _, err := s.CreateExecution(ctx, ne); err != nil {
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, owner, []string{queue}, 2, h.TTL)
	if err != nil || len(tasks) != 2 {
		t.Fatalf("limit 2: %d %v", len(tasks), err)
	}
	tasks, err = s.ClaimWorkflowTasks(ctx, owner, []string{queue}, 10, h.TTL)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("rest: %d %v", len(tasks), err)
	}
	tasks, err = s.ClaimWorkflowTasks(ctx, owner, []string{"other"}, 10, h.TTL)
	if err != nil || len(tasks) != 1 || tasks[0].Execution.InstanceID != "elsewhere" {
		t.Fatalf("other queue: %v %v", tasks, err)
	}
}

// The execution that became ready first is claimed first, whatever the
// creation order.
func testClaimOrdersByReadiness(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "older")
	started(t, s, claimOne(t, s, owner, h.TTL), nil) // older: no pending events
	h.Advance(t, s, 10*time.Millisecond)
	younger := mustCreate(t, s, "younger") // ready now
	h.Advance(t, s, 10*time.Millisecond)
	if err := s.Signal(ctx, "older", "s", nil); err != nil { // ready later
		t.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, owner, []string{queue}, 1, h.TTL)
	if err != nil || len(tasks) != 1 || tasks[0].Execution.ID != younger.ID {
		t.Fatalf("earliest ready first: %+v %v", tasks, err)
	}
}

// GW-2: a lease that expired and was granted again, even to the same owner,
// cannot complete.
func testGW2StaleLeaseSameOwner(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	stale := claimOne(t, s, owner, h.TTL)
	h.Advance(t, s, h.TTL+h.TTL/2)
	fresh := claimOne(t, s, owner, h.TTL)
	c := store.Completion{
		History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed: pendingIDs(stale),
	}
	if _, err := s.CompleteWorkflowTask(ctx, stale.Lease, &c); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale complete: %v", err)
	}
	if err := s.ExtendWorkflowTask(ctx, stale.Lease, h.TTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale extend: %v", err)
	}
	if err := s.ReleaseWorkflowTask(ctx, stale.Lease, 0); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale release: %v", err)
	}
	if err := s.ExtendWorkflowTask(ctx, fresh.Lease, h.TTL); err != nil {
		t.Fatalf("fresh extend: %v", err)
	}
	started(t, s, fresh, nil)
	if _, err := s.CompleteWorkflowTask(ctx, fresh.Lease, &store.Completion{}); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("complete after release: %v", err)
	}
}

func testCompleteAppendsHistory(t *testing.T, h Harness) {
	s := h.New(t)
	e := mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{History: []history.Event{
		{Type: history.TaskStarted, At: task.Now},
		{Type: history.SideEffectResult, ScheduleID: 1, Payload: []byte("7")},
	}})
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("an execution without pending events was claimed")
	}
	hist, err := s.LoadHistory(ctx, e.ID, 0)
	if err != nil || len(hist) != 3 {
		t.Fatalf("history %v %v", hist, err)
	}
	if hist[0].Type != history.ExecutionStarted || hist[2].Seq != 3 || string(hist[2].Payload) != "7" {
		t.Fatalf("history %+v", hist)
	}
	if !hist[1].At.Equal(task.Now) {
		t.Fatalf("At %v, want %v (microsecond precision round trip)", hist[1].At, task.Now)
	}
	tail, err := s.LoadHistory(ctx, e.ID, 2)
	if err != nil || len(tail) != 1 || tail[0].Seq != 3 {
		t.Fatalf("tail %v %v", tail, err)
	}
	if err := s.Signal(ctx, "i1", "go", nil); err != nil {
		t.Fatal(err)
	}
	next := claimOne(t, s, owner, h.TTL)
	if next.LastSeq != 3 {
		t.Fatalf("LastSeq %d", next.LastSeq)
	}
}

func testCompleteIsAtomic(t *testing.T, h Harness) {
	s := h.New(t)
	e := mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	c := store.Completion{
		History:    []history.Event{{Seq: 5, Type: history.ExecutionStarted}}, // gap
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}},
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Fatalf("gap in sequence: %v", err)
	}
	if hist, _ := s.LoadHistory(ctx, e.ID, 0); len(hist) != 0 {
		t.Fatalf("partial apply: history %v", hist)
	}
	if acts, _ := s.ClaimActivityTasks(ctx, owner, []string{queue}, 10, h.TTL); len(acts) != 0 {
		t.Fatal("partial apply: activity created")
	}
	started(t, s, task, nil) // the lease is still valid
}

func testTerminalStatus(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{
		History: []history.Event{{Type: history.ExecutionCompleted, Payload: []byte(`"ok"`)}},
		Status:  store.StatusCompleted, Output: []byte(`"ok"`),
	})
	e, err := s.GetExecution(ctx, "i1")
	if err != nil || e.Status != store.StatusCompleted || string(e.Output) != `"ok"` || e.EndedAt.IsZero() {
		t.Fatalf("ended execution %+v %v", e, err)
	}
	if err := s.Signal(ctx, "i1", "late", nil); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("signal to ended: %v", err)
	}
	again, created, err := s.CreateExecution(ctx, newExec("i1"))
	if err != nil || created || again.Status != store.StatusCompleted {
		t.Fatalf("create after end must return the ended execution: %+v %v %v", again, created, err)
	}
}

func testKeepHoldsLease(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{KeepFor: h.TTL})
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("kept lease was claimable")
	}
	if err := s.ExtendWorkflowTask(ctx, task.Lease, h.TTL); err != nil {
		t.Fatalf("kept lease must stay valid: %v", err)
	}
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	claimOne(t, s, other, h.TTL)
}

func testReleaseWithDelay(t *testing.T, h Harness) {
	// The delay (h.TTL) must last until the claim that follows the release.
	realTime(t, h, func(h Harness) bool {
		s := h.New(t)
		mustCreate(t, s, "i1")
		task := claimOne(t, s, owner, lease(h))
		inTime := within(t, s, h.TTL)
		if err := s.ReleaseWorkflowTask(ctx, task.Lease, h.TTL); err != nil {
			t.Fatal(err)
		}
		if got := claim(t, s, other, h.TTL); len(got) != 0 {
			if !inTime() {
				return false
			}
			t.Fatal("claimed during the release delay")
		}
		h.Advance(t, s, h.TTL+h.TTL/2)
		if got := claimOne(t, s, other, h.TTL); len(got.Pending) != 1 {
			t.Fatalf("pending after release: %v", pendingTypes(got))
		}
		return true
	})
}

func testSticky(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{StickyFor: h.TTL})
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("another owner claimed a sticky execution")
	}
	mine := claimOne(t, s, owner, h.TTL)
	if err := s.ReleaseWorkflowTask(ctx, mine.Lease, 0); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	claimOne(t, s, other, h.TTL)
}

func testTimers(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	at := task.Now.Add(h.TTL)
	started(t, s, task, &store.Completion{
		History: []history.Event{
			{Type: history.TimerScheduled, ScheduleID: 1, At: at},
			{Type: history.TimerScheduled, ScheduleID: 2, At: at},
		},
		Timers: []history.Event{{ScheduleID: 1, At: at, Name: "t1"}, {ScheduleID: 2, At: at, Name: "t2"}},
	})
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("timer visible before its time")
	}
	h.Advance(t, s, h.TTL/4)
	// Cancel timer 2 in a task triggered by a signal.
	if err := s.Signal(ctx, "i1", "cancel", nil); err != nil {
		t.Fatal(err)
	}
	sig := claimOne(t, s, owner, h.TTL)
	if sig.Now.Before(at) { // on a loaded machine the claim (or the signal) may come late
		if got := pendingTypes(sig); !slices.Equal(got, []history.Type{history.SignalReceived}) {
			t.Fatalf("pending %v", got)
		}
	}
	// Late, the timers are visible too, before or after the signal
	// depending on when it was sent: deliver the signal only.
	i := slices.IndexFunc(sig.Pending, func(p store.PendingEvent) bool { return p.Event.Type == history.SignalReceived })
	if i < 0 {
		t.Fatalf("pending %v", pendingTypes(sig))
	}
	c := store.Completion{
		History:        []history.Event{{Seq: sig.LastSeq + 1, Type: history.TimerCanceled, ScheduleID: 2}},
		Consumed:       []int64{sig.Pending[i].ID}, // the signal only
		CanceledTimers: []int32{2},
	}
	if _, err := s.CompleteWorkflowTask(ctx, sig.Lease, &c); err != nil {
		t.Fatal(err)
	}
	h.Advance(t, s, h.TTL)
	fired := claimOne(t, s, owner, h.TTL)
	if len(fired.Pending) != 1 || fired.Pending[0].Event.Type != history.TimerFired || fired.Pending[0].Event.ScheduleID != 1 {
		t.Fatalf("fired %+v", fired.Pending)
	}
}

func scheduleActivity(t *testing.T, s store.Store, h Harness, id string, a store.NewActivityTask) []store.Lease {
	t.Helper()
	mustCreate(t, s, id)
	task := claimOne(t, s, owner, h.TTL)
	c := store.Completion{
		History:    []history.Event{task.Pending[0].Event, {Type: history.ActivityScheduled, ScheduleID: a.ScheduleID, Name: a.Name}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{a},
	}
	c.History[0].Seq, c.History[1].Seq = 1, 2
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &c)
	if err != nil {
		t.Fatal(err)
	}
	return leases
}

func claimActivity(t *testing.T, s store.Store, who string, ttl time.Duration) []store.ActivityTask {
	t.Helper()
	acts, err := s.ClaimActivityTasks(ctx, who, []string{queue}, 10, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return acts
}

func testActivityRoundTrip(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "charge", Queue: queue, Input: []byte(`{"a":1}`)})
	acts := claimActivity(t, s, owner, h.TTL)
	if len(acts) != 1 {
		t.Fatalf("claimed %d activities", len(acts))
	}
	a := acts[0]
	if a.Name != "charge" || a.ScheduleID != 1 || a.Attempt != 1 || string(a.Input) != `{"a":1}` || a.InstanceID != "i1" {
		t.Fatalf("activity %+v", a)
	}
	if got := claimActivity(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("claimed an activity with a live lease")
	}
	res := store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted, Payload: []byte(`"done"`), CompData: []byte(`{"refund":1}`)}}
	if _, err := s.CompleteActivityTask(ctx, a.Lease, res); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if len(task.Pending) != 1 {
		t.Fatalf("pending %v", pendingTypes(task))
	}
	ev := task.Pending[0].Event
	if ev.Type != history.ActivityCompleted || ev.ScheduleID != 1 || ev.Attempt != 1 || string(ev.Payload) != `"done"` || string(ev.CompData) != `{"refund":1}` {
		t.Fatalf("result %+v", ev)
	}
	if got := claimActivity(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("completed activity task still claimable")
	}
}

// GW-2: an activity whose lease expired and was re-granted cannot be
// extended (the worker must learn it lost the task) nor completed.
func testActivityLeaseFenced(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	first := claimActivity(t, s, owner, h.TTL)[0]
	if err := s.ExtendActivityTask(ctx, first.Lease, h.TTL); err != nil {
		t.Fatalf("live extend: %v", err)
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	second := claimActivity(t, s, owner, h.TTL)
	if len(second) != 1 || second[0].Attempt != 2 || second[0].Lease.Version <= first.Lease.Version {
		t.Fatalf("reclaim %+v", second)
	}
	if err := s.ExtendActivityTask(ctx, first.Lease, h.TTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale extend: %v", err)
	}
	res := store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}
	if _, err := s.CompleteActivityTask(ctx, first.Lease, res); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale complete: %v", err)
	}
	if err := s.RetryActivityTask(ctx, first.Lease, 0, false, nil); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale retry: %v", err)
	}
	if _, err := s.CompleteActivityTask(ctx, second[0].Lease, res); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if len(task.Pending) != 1 || task.Pending[0].Event.Attempt != 2 {
		t.Fatalf("exactly one result, from attempt 2: %+v", task.Pending)
	}
}

func testActivityRetry(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if err := s.RetryActivityTask(ctx, a.Lease, h.TTL, false, nil); err != nil {
		t.Fatal(err)
	}
	if got := claimActivity(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("retry visible before its delay")
	}
	h.Advance(t, s, h.TTL+h.TTL/2)
	got := claimActivity(t, s, other, h.TTL)
	if len(got) != 1 || got[0].Attempt != 2 {
		t.Fatalf("retry %+v", got)
	}
}

func testEagerActivityClaim(t *testing.T, h Harness) {
	s := h.New(t)
	leases := scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL})
	if len(leases) != 1 || leases[0].Owner != owner {
		t.Fatalf("eager leases %+v", leases)
	}
	if got := claimActivity(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("eagerly claimed task was claimable")
	}
	res := store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}}
	if _, err := s.CompleteActivityTask(ctx, leases[0], res); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if task.Pending[0].Event.Attempt != 1 {
		t.Fatalf("eager attempt %d", task.Pending[0].Event.Attempt)
	}
}

func testActivityResultDropped(t *testing.T, h Harness) {
	s := h.New(t)
	scheduleActivity(t, s, h, "i1", store.NewActivityTask{ScheduleID: 1, Name: "a", Queue: queue})
	a := claimActivity(t, s, owner, h.TTL)[0]
	if err := s.RequestCancel(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	c := store.Completion{
		History:  []history.Event{{Seq: task.LastSeq + 1, Type: history.ExecutionCanceled}},
		Consumed: pendingIDs(task),
		Status:   store.StatusCanceled,
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); err != nil {
		t.Fatal(err)
	}
	// The task of an ended execution is gone: completing it is a lost lease
	// or a silent drop, never a delivery.
	_, err := s.CompleteActivityTask(ctx, a.Lease, store.ActivityResult{Event: history.Event{Type: history.ActivityCompleted}})
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("complete for ended execution: %v", err)
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("ended execution became claimable")
	}
}

func testSignals(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.Signal(ctx, "missing", "x", nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown instance: %v", err)
	}
	mustCreate(t, s, "i1")
	started(t, s, claimOne(t, s, owner, h.TTL), nil)
	const n = 150 // go-workflows dropped signals past 100 (GW-14)
	for i := range n {
		if err := s.Signal(ctx, "i1", "s", fmt.Append(nil, i)); err != nil {
			t.Fatal(err)
		}
	}
	task := claimOne(t, s, owner, h.TTL)
	if len(task.Pending) != n {
		t.Fatalf("pending %d, want %d", len(task.Pending), n)
	}
	for i, p := range task.Pending {
		if p.Event.Type != history.SignalReceived || p.Event.Name != "s" || string(p.Event.Payload) != strconv.Itoa(i) {
			t.Fatalf("signal %d: %+v (GW-14: FIFO)", i, p.Event)
		}
	}
}

func testCancelRequest(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.RequestCancel(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}
	mustCreate(t, s, "ended")
	started(t, s, claimOne(t, s, owner, h.TTL), &store.Completion{Status: store.StatusCompleted})
	if err := s.RequestCancel(ctx, "ended"); !errors.Is(err, store.ErrNotActive) {
		t.Fatalf("ended: %v", err)
	}
	mustCreate(t, s, "i1")
	started(t, s, claimOne(t, s, owner, h.TTL), nil)
	if err := s.RequestCancel(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	task := claimOne(t, s, owner, h.TTL)
	if got := pendingTypes(task); !slices.Equal(got, []history.Type{history.ExecutionCancelRequested}) {
		t.Fatalf("pending %v", got)
	}
}

// GW-3: continue-as-new moves undelivered signals to the next execution.
func testContinueAsNewCarriesSignals(t *testing.T, h Harness) {
	s := h.New(t)
	first := mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	// A signal arrives while the task runs: not delivered to this task.
	if err := s.Signal(ctx, "i1", "during", []byte("1")); err != nil {
		t.Fatal(err)
	}
	next := newExec("ignored")
	next.Input = []byte(`{"n":2}`)
	started(t, s, task, &store.Completion{
		History:       []history.Event{{Type: history.ExecutionContinuedAsNew, Payload: next.Input}},
		Status:        store.StatusContinued,
		ContinueAsNew: &next,
	})
	ended, err := s.GetExecution(ctx, "i1")
	if err != nil || ended.ID == first.ID || ended.Status != store.StatusRunning || string(ended.Input) != `{"n":2}` {
		t.Fatalf("latest execution %+v %v", ended, err)
	}
	t2 := claimOne(t, s, owner, h.TTL)
	if t2.Execution.ID == first.ID {
		t.Fatal("claimed the continued execution")
	}
	if got := pendingTypes(t2); !slices.Equal(got, []history.Type{history.ExecutionStarted, history.SignalReceived}) {
		t.Fatalf("next execution pending %v", got)
	}
}

// GW-3: signals sent concurrently with the completion that ends an
// execution are either delivered to it (before) or rejected as not active
// (after), never accepted and lost.
func testSignalRacingCompletion(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	var wg sync.WaitGroup
	accepted := make([]bool, 20)
	for i := range accepted {
		wg.Go(func() {
			// The instance stays active across continue-as-new: every
			// signal must be accepted.
			err := s.Signal(ctx, "i1", "s", nil)
			accepted[i] = err == nil
			if err != nil {
				t.Errorf("signal racing continue-as-new: %v", err)
			}
		})
	}
	next := newExec("ignored")
	started(t, s, task, &store.Completion{
		History:       []history.Event{{Type: history.ExecutionContinuedAsNew}},
		Status:        store.StatusContinued,
		ContinueAsNew: &next,
	})
	wg.Wait()
	want := 0
	for _, a := range accepted {
		if a {
			want++
		}
	}
	// Every accepted signal is pending on the latest execution (the first
	// task consumed only the start event).
	got := 0
	for _, tk := range claim(t, s, owner, h.TTL) {
		for _, p := range tk.Pending {
			if p.Event.Type == history.SignalReceived {
				got++
			}
		}
	}
	if got != want {
		t.Fatalf("%d signals accepted, %d delivered", want, got)
	}
}

func testChildren(t *testing.T, h Harness) {
	s := h.New(t)
	parent := mustCreate(t, s, "parent")
	taken := newExec("taken")
	taken.Queue = "other" // keeps it out of this test's claims
	if _, _, err := s.CreateExecution(ctx, taken); err != nil {
		t.Fatal(err)
	}
	ptask := claimOne(t, s, owner, h.TTL)
	if ptask.Execution.ID != parent.ID {
		t.Fatalf("claimed %s", ptask.Execution.InstanceID)
	}
	child := store.NewExecution{InstanceID: "child", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}
	dup := store.NewExecution{InstanceID: "taken", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 2}
	started(t, s, ptask, &store.Completion{
		History: []history.Event{
			{Type: history.ChildScheduled, ScheduleID: 1, Ref: "child"},
			{Type: history.ChildScheduled, ScheduleID: 2, Ref: "taken"},
		},
		Children: []store.NewExecution{child, dup},
	})
	ce, err := s.GetExecution(ctx, "child")
	if err != nil || ce.ParentID != parent.ID || ce.ParentSched != 1 || ce.Workflow != "cw" {
		t.Fatalf("child %+v %v", ce, err)
	}
	// The duplicate child failed at once: the next claim gets the parent
	// (ChildFailed) and the child (its start).
	ctask := claimChildAfterDuplicate(t, s, h)
	if ctask.Lease.ID == 0 {
		t.Fatal("child not claimed")
	}
	started(t, s, ctask, &store.Completion{
		History: []history.Event{{Type: history.ExecutionCompleted, Payload: []byte(`"child-out"`)}},
		Status:  store.StatusCompleted, Output: []byte(`"child-out"`),
	})
	tk := claimOne(t, s, owner, h.TTL)
	if tk.Execution.ID != parent.ID || len(tk.Pending) != 1 {
		t.Fatalf("parent notified: %+v", tk)
	}
	ev := tk.Pending[0].Event
	if ev.Type != history.ChildCompleted || ev.ScheduleID != 1 || string(ev.Payload) != `"child-out"` {
		t.Fatalf("child completion %+v", ev)
	}
	// Cancel request to a child.
	if _, err := s.CompleteWorkflowTask(ctx, tk.Lease, &store.Completion{
		History:      []history.Event{{Seq: tk.LastSeq + 1, Type: history.ChildCompleted, ScheduleID: 1}},
		Consumed:     pendingIDs(tk),
		ChildCancels: []string{"taken"},
	}); err != nil {
		t.Fatal(err)
	}
	// "taken" is not a child of this execution: the cancel is ignored.
	others, err := s.ClaimWorkflowTasks(ctx, owner, []string{"other"}, 10, h.TTL)
	if err != nil || len(others) != 1 || slices.Contains(pendingTypes(others[0]), history.ExecutionCancelRequested) {
		t.Fatalf("cancel reached a non-child: %+v %v", others, err)
	}
}

// claimChildAfterDuplicate handles the parent's ChildFailed for the
// duplicate id and returns the claimed task of the new child.
func claimChildAfterDuplicate(t *testing.T, s store.Store, h Harness) store.WorkflowTask {
	var ctask store.WorkflowTask
	for _, tk := range claim(t, s, owner, h.TTL) {
		switch tk.Execution.InstanceID {
		case "parent":
			ev := tk.Pending[0].Event
			if len(tk.Pending) != 1 || ev.Type != history.ChildFailed || ev.ScheduleID != 2 || ev.ErrKinds != store.KindInstanceExists {
				t.Fatalf("parent pending %+v", tk.Pending)
			}
			if _, err := s.CompleteWorkflowTask(ctx, tk.Lease, &store.Completion{
				History:  []history.Event{{Seq: tk.LastSeq + 1, Type: history.ChildFailed, ScheduleID: 2}},
				Consumed: pendingIDs(tk),
			}); err != nil {
				t.Fatal(err)
			}
		case "child":
			ctask = tk
		}
	}
	return ctask
}

func complete(t *testing.T, s store.Store, task store.WorkflowTask, c store.Completion) {
	t.Helper()
	for i := range c.History {
		c.History[i].Seq = task.LastSeq + int64(i) + 1
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func testChildOutcomeKinds(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "parent")
	ptask := claimOne(t, s, owner, h.TTL)
	child := store.NewExecution{InstanceID: "child", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 3}
	started(t, s, ptask, &store.Completion{Children: []store.NewExecution{child}})
	// The parent cancels its child in a later task.
	if err := s.Signal(ctx, "parent", "stop-child", nil); err != nil {
		t.Fatal(err)
	}
	for _, tk := range claim(t, s, owner, h.TTL) {
		switch tk.Execution.InstanceID {
		case "parent":
			complete(t, s, tk, store.Completion{Consumed: pendingIDs(tk), ChildCancels: []string{"child"}})
		case "child": // claimed with its start only: give it back
			if err := s.ReleaseWorkflowTask(ctx, tk.Lease, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	ct := claimOne(t, s, owner, h.TTL)
	if got := pendingTypes(ct); !slices.Equal(got, []history.Type{history.ExecutionStarted, history.ExecutionCancelRequested}) {
		t.Fatalf("child pending %v", got)
	}
	// The child continues as new (no notification to the parent), then is
	// canceled.
	next := child
	started(t, s, ct, &store.Completion{Status: store.StatusContinued, ContinueAsNew: &next})
	ct2 := claimOne(t, s, owner, h.TTL)
	if ct2.Execution.InstanceID != "child" || ct2.Execution.ParentSched != 3 {
		t.Fatalf("continued child keeps its parent link: %+v", ct2.Execution)
	}
	started(t, s, ct2, &store.Completion{Status: store.StatusCanceled})
	pt := claimOne(t, s, owner, h.TTL)
	if len(pt.Pending) != 1 {
		t.Fatalf("parent pending %v", pendingTypes(pt))
	}
	ev := pt.Pending[0].Event
	if ev.Type != history.ChildFailed || ev.ScheduleID != 3 || ev.ErrKinds != store.KindCanceled {
		t.Fatalf("canceled child outcome %+v", ev)
	}
}

// GW-3: a signal accepted while a task runs is not dropped when that task
// ends the execution: the completion is rejected until it is delivered.
func testTerminalWithPendingSignal(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if err := s.Signal(ctx, "i1", "late", []byte("1")); err != nil {
		t.Fatal(err)
	}
	c := store.Completion{
		History:  []history.Event{task.Pending[0].Event, {Type: history.ExecutionCompleted}},
		Consumed: pendingIDs(task),
		Status:   store.StatusCompleted,
	}
	c.History[0].Seq, c.History[1].Seq = 1, 2
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); !errors.Is(err, store.ErrPendingEvents) {
		t.Fatalf("terminal completion with an undelivered signal: %v", err)
	}
	if hist, _ := s.LoadHistory(ctx, task.Execution.ID, 0); len(hist) != 0 {
		t.Fatal("rejected completion applied history")
	}
	// The executor gives up the task and runs a new one with the signal.
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	again := claimOne(t, s, owner, h.TTL)
	if got := pendingTypes(again); !slices.Equal(got, []history.Type{history.ExecutionStarted, history.SignalReceived}) {
		t.Fatalf("pending %v", got)
	}
	started(t, s, again, &store.Completion{
		History: []history.Event{again.Pending[1].Event, {Type: history.ExecutionCompleted}},
		Status:  store.StatusCompleted,
	})
	if e, _ := s.GetExecution(ctx, "i1"); e.Status != store.StatusCompleted {
		t.Fatalf("status %v", e.Status)
	}
}

// GW-3: continue-as-new carries an undelivered cancel request.
func testContinueAsNewCarriesCancel(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if err := s.RequestCancel(ctx, "i1"); err != nil {
		t.Fatal(err)
	}
	next := newExec("ignored")
	started(t, s, task, &store.Completion{Status: store.StatusContinued, ContinueAsNew: &next})
	t2 := claimOne(t, s, owner, h.TTL)
	if got := pendingTypes(t2); !slices.Equal(got, []history.Type{history.ExecutionStarted, history.ExecutionCancelRequested}) {
		t.Fatalf("pending %v", got)
	}
}

// Pending events come in (visibility time, arrival) order: a timer created
// before a signal arrives but firing later comes after it.
func testPendingOrder(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	// Arrives while the task runs: pending, visible now.
	if err := s.Signal(ctx, "i1", "early", nil); err != nil {
		t.Fatal(err)
	}
	at := task.Now.Add(h.TTL / 2)
	started(t, s, task, &store.Completion{ // consumes only the start event
		History: []history.Event{{Type: history.TimerScheduled, ScheduleID: 1, At: at}},
		Timers:  []history.Event{{ScheduleID: 1, At: at}},
	})
	h.Advance(t, s, h.TTL)
	if err := s.Signal(ctx, "i1", "late", nil); err != nil { // after the timer fired
		t.Fatal(err)
	}
	got := claimOne(t, s, owner, h.TTL)
	var order []string
	for _, p := range got.Pending {
		order = append(order, p.Event.Type.String()+":"+p.Event.Name)
	}
	want := []string{"SignalReceived:early", "TimerFired:", "SignalReceived:late"}
	if !slices.Equal(order, want) {
		t.Fatalf("order %v, want %v", order, want)
	}
}

func testPartialConsume(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	if err := s.Signal(ctx, "i1", "a", nil); err != nil {
		t.Fatal(err)
	}
	started(t, s, task, nil) // consumes only the start event
	next := claimOne(t, s, owner, h.TTL)
	if got := pendingTypes(next); !slices.Equal(got, []history.Type{history.SignalReceived}) {
		t.Fatalf("pending %v", got)
	}
}

// The eager loop: keep the lease, run an eagerly claimed activity, complete
// it and resume the workflow in one call, without another owner seeing it.
func testEagerLoop(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}, {Seq: 2, Type: history.ActivityScheduled, ScheduleID: 1, Name: "a"}},
		Consumed:   pendingIDs(task),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil || len(leases) != 1 {
		t.Fatalf("complete: %v %v", leases, err)
	}
	if err := s.Signal(ctx, "i1", "meanwhile", nil); err != nil {
		t.Fatal(err)
	}
	if got := claim(t, s, other, h.TTL); len(got) != 0 {
		t.Fatal("kept execution claimed by another owner")
	}
	resumed, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{
		Event:  history.Event{Type: history.ActivityCompleted, Payload: []byte("1")},
		Resume: &task.Lease, ResumeFor: h.TTL,
	})
	if err != nil || resumed == nil {
		t.Fatalf("complete+resume: %v %v", resumed, err)
	}
	if resumed.LastSeq != 2 || resumed.Lease != task.Lease || resumed.Now.IsZero() {
		t.Fatalf("resumed task %+v", resumed)
	}
	if got := pendingTypes(*resumed); !slices.Equal(got, []history.Type{history.SignalReceived, history.ActivityCompleted}) {
		t.Fatalf("resumed pending %v", got)
	}
	// Nothing new: Resume returns an empty task, the lease stays.
	complete(t, s, *resumed, store.Completion{Consumed: pendingIDs(*resumed), KeepFor: h.TTL})
	empty, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL)
	if err != nil || len(empty.Pending) != 0 || empty.LastSeq != 2 {
		t.Fatalf("empty resume %+v %v", empty, err)
	}
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("resume after release: %v", err)
	}
	// A lost workflow lease: the activity result is recorded, no task.
	_, _, _ = s.CreateExecution(ctx, newExec("i2"))
	t2 := claimOne(t, s, owner, h.TTL)
	ls, err := s.CompleteWorkflowTask(ctx, t2.Lease, &store.Completion{
		History:    []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
		Consumed:   pendingIDs(t2),
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue, ClaimFor: h.TTL}},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteActivityTask(ctx, ls[0], store.ActivityResult{
		Event: history.Event{Type: history.ActivityCompleted}, Resume: &t2.Lease, ResumeFor: h.TTL,
	})
	if err != nil || res != nil {
		t.Fatalf("resume of a released lease: %v %v", res, err)
	}
	if got := claimOne(t, s, other, h.TTL); !slices.Equal(pendingTypes(got), []history.Type{history.ActivityCompleted}) {
		t.Fatalf("result not recorded: %v", pendingTypes(got))
	}
}

func testReleaseClearsSticky(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	started(t, s, claimOne(t, s, owner, h.TTL), &store.Completion{StickyFor: 10 * h.TTL})
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	mine := claimOne(t, s, owner, h.TTL)
	if err := s.ReleaseWorkflowTask(ctx, mine.Lease, 0); err != nil {
		t.Fatal(err)
	}
	claimOne(t, s, other, h.TTL) // not locked out by stale stickiness
}

func testExpiredLeaseStillValid(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	h.Advance(t, s, h.TTL+h.TTL/2)
	if err := s.ExtendWorkflowTask(ctx, task.Lease, h.TTL); err != nil {
		t.Fatalf("expired, not granted again: extend must succeed: %v", err)
	}
	started(t, s, task, nil)
}

// needs_attention executions keep their id and accept signals but are not
// claimed; compensating ones are claimed.
func testParkedStatuses(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "parked")
	started(t, s, claimOne(t, s, owner, h.TTL), &store.Completion{Status: store.StatusNeedsAttention, Err: "non-deterministic"})
	if e, _ := s.GetExecution(ctx, "parked"); e.Err != "non-deterministic" {
		t.Fatalf("park reason %q", e.Err)
	}
	if err := s.Signal(ctx, "parked", "x", nil); err != nil {
		t.Fatalf("signal to needs_attention: %v", err)
	}
	if got := claim(t, s, owner, h.TTL); len(got) != 0 {
		t.Fatal("needs_attention execution was claimed")
	}
	if _, created, _ := s.CreateExecution(ctx, newExec("parked")); created {
		t.Fatal("needs_attention execution lost its instance id")
	}
	mustCreate(t, s, "comp")
	ct := claimOne(t, s, owner, h.TTL)
	started(t, s, ct, &store.Completion{Status: store.StatusCompensating})
	if err := s.Signal(ctx, "comp", "x", nil); err != nil {
		t.Fatal(err)
	}
	if got := claimOne(t, s, owner, h.TTL); got.Execution.Status != store.StatusCompensating {
		t.Fatalf("compensating execution status %v", got.Execution.Status)
	}
}

func testInvalidCompletions(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	base := func() store.Completion {
		return store.Completion{History: []history.Event{{Seq: 1, Type: history.ExecutionStarted}}, Consumed: pendingIDs(task)}
	}
	can := base()
	can.ContinueAsNew = &store.NewExecution{Workflow: "wf", Queue: queue}
	dup := base()
	dup.Activities = []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}, {ScheduleID: 1, Name: "a", Queue: queue}}
	ended := base()
	ended.Status = store.StatusCompleted
	ended.Timers = []history.Event{{ScheduleID: 1, At: task.Now}}
	zero := base()
	zero.History[0].Seq = 0
	for name, c := range map[string]store.Completion{
		"continue-as-new without status": can, "duplicate schedule id": dup, "timer in a terminal completion": ended,
		"history starting at seq 0": zero,
	} {
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); !errors.Is(err, store.ErrInvalidCompletion) {
			t.Errorf("%s: %v", name, err)
		}
	}
	ok := base()
	ok.Activities = []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}}
	ok.KeepFor = h.TTL
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &ok); err != nil {
		t.Fatal(err)
	}
	// A second task for an existing schedule id, after other changes in the
	// same completion: rejected as a whole (Postgres fails mid-batch).
	again := store.Completion{
		History:    []history.Event{{Seq: 2, Type: history.TimerScheduled, ScheduleID: 2, At: task.Now}},
		Timers:     []history.Event{{ScheduleID: 2, At: task.Now}},
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: queue}},
	}
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &again); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Errorf("task for an existing schedule id: %v", err)
	}
	if hist, _ := s.LoadHistory(ctx, task.Execution.ID, 1); len(hist) != 0 {
		t.Errorf("partial apply: %+v", hist)
	}
	if err := s.ReleaseWorkflowTask(ctx, task.Lease, 0); err != nil {
		t.Fatal(err)
	}
	a := claimActivity(t, s, owner, h.TTL)[0]
	bad := store.ActivityResult{Event: history.Event{Type: history.SignalReceived}}
	if _, err := s.CompleteActivityTask(ctx, a.Lease, bad); !errors.Is(err, store.ErrInvalidCompletion) {
		t.Errorf("result of the wrong type: %v", err)
	}
	// Resume with another execution's lease: ignored (no task).
	mustCreate(t, s, "i2")
	t2 := claimOne(t, s, owner, h.TTL)
	res, err := s.CompleteActivityTask(ctx, a.Lease, store.ActivityResult{
		Event: history.Event{Type: history.ActivityCompleted}, Resume: &t2.Lease, ResumeFor: h.TTL,
	})
	if err != nil || res != nil {
		t.Errorf("resume of a foreign lease: %v %v", res, err)
	}
	if _, err := s.LoadHistory(ctx, 1<<40, 0); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("history of an unknown execution: %v", err)
	}
}

func testActivityQueuesAndLimit(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Activities: []store.NewActivityTask{
		{ScheduleID: 1, Name: "a", Queue: queue},
		{ScheduleID: 2, Name: "a", Queue: queue},
		{ScheduleID: 3, Name: "a", Queue: queue},
		{ScheduleID: 4, Name: "b", Queue: "other"},
	}})
	got, err := s.ClaimActivityTasks(ctx, owner, []string{queue}, 2, h.TTL)
	if err != nil || len(got) != 2 {
		t.Fatalf("limit: %d %v", len(got), err)
	}
	if got := claimActivity(t, s, owner, h.TTL); len(got) != 1 {
		t.Fatalf("rest: %d", len(got))
	}
	other, err := s.ClaimActivityTasks(ctx, owner, []string{"other"}, 10, h.TTL)
	if err != nil || len(other) != 1 || other[0].Name != "b" {
		t.Fatalf("other queue: %+v %v", other, err)
	}
}

// T8.4 review: a queue listed twice (or several queues) does not shrink a
// claim: each ready task is claimed once, up to the limit.
func testClaimsWithRepeatedQueues(t *testing.T, h Harness) {
	s := h.New(t)
	for _, id := range []string{"i1", "i2", "i3"} {
		mustCreate(t, s, id)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, owner, []string{queue, queue, "other", queue}, 3, h.TTL)
	if err != nil || len(tasks) != 3 {
		t.Fatalf("workflow claim: %d %v", len(tasks), err)
	}
	seen := map[int64]bool{}
	for _, tk := range tasks {
		if seen[tk.Execution.ID] || len(tk.Pending) != 1 {
			t.Fatalf("claimed %+v", tasks)
		}
		seen[tk.Execution.ID] = true
	}
	started(t, s, tasks[0], &store.Completion{Activities: []store.NewActivityTask{
		{ScheduleID: 1, Name: "a", Queue: queue},
		{ScheduleID: 2, Name: "a", Queue: queue},
		{ScheduleID: 3, Name: "a", Queue: queue},
	}})
	acts, err := s.ClaimActivityTasks(ctx, owner, []string{queue, queue, queue}, 3, h.TTL)
	if err != nil || len(acts) != 3 || acts[0].ScheduleID == acts[1].ScheduleID {
		t.Fatalf("activity claim: %+v %v", acts, err)
	}
}

func testDefensiveCopies(t *testing.T, h Harness) {
	s := h.New(t)
	ne := newExec("i1")
	e, _, err := s.CreateExecution(ctx, ne)
	if err != nil {
		t.Fatal(err)
	}
	ne.Input[0], e.Input[0] = 'X', 'Y'
	task := claimOne(t, s, owner, h.TTL)
	task.Pending[0].Event.Payload[0] = 'Z'
	got, _ := s.GetExecution(ctx, "i1")
	if string(got.Input) != `{"n":1}` {
		t.Fatalf("input changed through a caller's slice: %q", got.Input)
	}
	ev := task.Pending[0].Event
	ev.Payload = []byte("p")
	c := store.Completion{History: []history.Event{ev}, Consumed: pendingIDs(task)}
	c.History[0].Seq = 1
	if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); err != nil {
		t.Fatal(err)
	}
	ev.Payload[0] = 'q'
	hist, _ := s.LoadHistory(ctx, e.ID, 0)
	if string(hist[0].Payload) != "p" {
		t.Fatalf("history changed through a caller's slice: %q", hist[0].Payload)
	}
}

// GW-5: timestamps come from the store clock, not the caller's.
func testStoreClock(t *testing.T, h Harness) {
	s := h.New(t)
	before, err := s.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Status: store.StatusCompleted})
	after, _ := s.Now(ctx)
	e, _ := s.GetExecution(ctx, "i1")
	for name, ts := range map[string]time.Time{"created": e.CreatedAt, "task": task.Now, "ended": e.EndedAt} {
		if ts.Before(before) || ts.After(after) {
			t.Errorf("%s at %v, outside store time [%v, %v]", name, ts, before, after)
		}
	}
}

// GW-3: signals racing a terminal completion are each either delivered
// before it (and the completion is rejected until they are seen), or
// rejected as not active; never accepted and lost.
func testSignalsRacingTerminal(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for range 20 {
		wg.Go(func() {
			err := s.Signal(ctx, "i1", "s", nil)
			if err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			} else if !errors.Is(err, store.ErrNotActive) {
				t.Errorf("signal: %v", err)
			}
		})
	}
	// Complete, resuming to take in signals as long as the store says some
	// are pending; count every signal delivered to the workflow.
	delivered := 0
	cur := task
	c := store.Completion{History: []history.Event{task.Pending[0].Event}, Consumed: pendingIDs(task)}
	for {
		c.Status = store.StatusCompleted
		for i := range c.History {
			c.History[i].Seq = cur.LastSeq + int64(i) + 1
		}
		_, err := s.CompleteWorkflowTask(ctx, cur.Lease, &c)
		if err == nil {
			break
		}
		if !errors.Is(err, store.ErrPendingEvents) {
			t.Fatal(err)
		}
		next, err := s.ResumeWorkflowTask(ctx, cur.Lease, h.TTL)
		if err != nil {
			t.Fatal(err)
		}
		// next.LastSeq is unchanged: the rejected completion applied
		// nothing; deliver the old and the new events together.
		cur.LastSeq = next.LastSeq
		for _, p := range next.Pending {
			if !slices.Contains(c.Consumed, p.ID) {
				c.Consumed = append(c.Consumed, p.ID)
				c.History = append(c.History, p.Event)
				if p.Event.Type == history.SignalReceived {
					delivered++
				}
			}
		}
	}
	wg.Wait()
	if delivered != accepted {
		t.Fatalf("%d signals accepted, %d delivered", accepted, delivered)
	}
}

func testParkedCannotKeep(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "i1")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{Status: store.StatusNeedsAttention, KeepFor: h.TTL})
	if _, err := s.ResumeWorkflowTask(ctx, task.Lease, h.TTL); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("resume of a parked execution: %v", err)
	}
}
