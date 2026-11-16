// Package flowtest runs workflows in tests: the real engine (worker,
// executor, compensation, locks) on an in-memory store, with virtual time
// and mocked activities.
//
//	func TestOrder(t *testing.T) {
//		tst := flowtest.New(t)
//		flowtest.OnActivity(tst, charge, func(ctx context.Context, k flows.Key, o Order) (string, string, error) {
//			return "ch_1", "ch_1", nil // output, compensation data
//		})
//		r, err := flowtest.Run(tst, placeOrder, "order-1", Order{ID: "1"})
//		...
//	}
//
// Virtual time moves only when the engine is idle: the clock jumps to the
// next timer, retry or scheduled signal, so a workflow that sleeps a week
// runs in milliseconds, and nothing depends on the wall clock (go-workflows
// tester issues #494, #481). A workflow that waits for something that can
// never come fails Run with ErrBlocked, at once.
//
// Run reports the activities and children started but not awaited (#436),
// by one rule: each ended execution is reported exactly once, at the first
// Run whose tree contains it. The tree of Run(id) is every execution whose
// instance id is id (whatever its parent, earlier executions of a
// continue-as-new chain included, even if started before Run) plus every
// execution whose parent is in the tree. Only ended executions are
// checked: one still running when Run returns (a child detached by
// continue-as-new) is checked by a later Run whose tree contains it, if
// any.
//
// Activities run for real unless mocked; their own sleeps take wall time,
// and virtual time does not move while one runs: an activity takes no
// virtual time, whatever the speed of the machine. So an activity that
// waits for its context to be canceled by a workflow timer (Select over a
// timer and the activity, canceling it) never sees the timer fire, and Run
// fails with ErrNeverIdle after 30s of wall time. This is a limitation, not
// a bug to be fixed by moving time anyway: the tester cannot tell such an
// activity from a slow one, and moving time under a running activity would
// expire its lease and timeouts, making the outcome depend on the wall
// clock. Mock the activity to return, or model the wait in the workflow (a
// signal, a child workflow, a timer). Note that a mock makes the activity's
// branch win the Select: the branch where the timer wins cannot be tested
// with flowtest while the other case is an activity.
//
// Run, Advance and Settle must be called from the test's goroutine.
package flowtest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/bridge"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// ErrBlocked reports a workflow waiting for something that can never
// happen: no timer, retry, task or scheduled signal is left.
var ErrBlocked = errors.New("flowtest: workflow blocked: nothing left to wait for")

// Tester runs workflows on an in-memory engine with virtual time.
type Tester struct {
	t     testing.TB
	clock *mem.ManualClock
	st    *mem.Store
	e     *flows.Engine
	w     *worker.Worker
	cfg   config

	mu      sync.Mutex
	mocks   map[mockKey]*worker.Activity
	signals []scheduledSignal
	queues  []string
	// dropped: what executions started in their last task without awaiting
	// it (not in their histories: never started), by execution id, until
	// reported.
	dropped map[int64]string
	// reported: the ended executions already checked by a Run (each is
	// reported once).
	reported map[int64]bool
}

type mockKey struct {
	name       string
	compensate bool
}

type scheduledSignal struct {
	at             time.Time
	instance, name string
	payload        any
}

type config struct {
	start        time.Time
	allowPending bool
	options      flows.Options
}

// Option configures a Tester.
type Option func(*config)

// WithStart sets the virtual time the test starts at (default: the Unix
// epoch plus one day).
func WithStart(t time.Time) Option { return func(c *config) { c.start = t } }

// AllowPendingFutures stops Run from failing the test when the workflow
// ended with activities or children it started and did not await.
func AllowPendingFutures() Option { return func(c *config) { c.allowPending = true } }

// WithOptions sets engine options (queues, hooks, observer, ...).
// InstanceID, PollMax and LeaseTTL are set by the tester.
func WithOptions(o flows.Options) Option { return func(c *config) { c.options = o } }

// New starts a Tester, stopped when the test ends.
func New(tb testing.TB, opts ...Option) *Tester {
	tb.Helper()
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	tst := &Tester{t: tb, clock: &mem.ManualClock{}, cfg: cfg, mocks: map[mockKey]*worker.Activity{}}
	if !cfg.start.IsZero() {
		tst.clock.Advance(cfg.start.Sub(tst.clock.Now()))
	}
	tst.st = mem.New(tst.clock)
	o := cfg.options
	// Leases renew on wall time (every third of the TTL) but expire on
	// virtual time, which only moves while the engine is idle. The TTL
	// bounds how late a cancel (reported by renewals) reaches a polled
	// activity, and how late a renewal may be on a loaded machine.
	o.InstanceID, o.PollMax, o.LeaseTTL = "flowtest", 2*time.Millisecond, 10*time.Second
	tst.queues = o.Queues
	if len(tst.queues) == 0 {
		tst.queues = []string{flows.DefaultQueue}
	}
	e, w, err := bridge.NewEngine(tst.st, tst.mock, o)
	if err != nil {
		tb.Fatalf("flowtest: %v", err)
	}
	engine, ok := e.(*flows.Engine)
	if !ok {
		tb.Fatalf("flowtest: bridge returned %T", e)
	}
	tst.e, tst.w = engine, w
	w.SetDroppedHook(func(_ string, executionID int64, acts, kids int) {
		if cfg.allowPending {
			return // never reported
		}
		tst.mu.Lock()
		defer tst.mu.Unlock()
		if tst.dropped == nil {
			tst.dropped = map[int64]string{}
		}
		tst.dropped[executionID] = fmt.Sprintf("%d activities and %d children started in the last task (not started)", acts, kids)
	})
	if err := tst.e.Start(context.Background()); err != nil {
		tb.Fatalf("flowtest: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = tst.e.Stop(ctx)
	})
	return tst
}

func (tst *Tester) mock(name string, compensate bool) (*worker.Activity, bool) {
	tst.mu.Lock()
	defer tst.mu.Unlock()
	a, ok := tst.mocks[mockKey{name, compensate}]
	return a, ok
}

// OnActivity runs fn instead of act's invocation in this tester, with
// act's options (timeout, retries): fn returns the output and the
// compensation data, stored when act has a compensator.
func OnActivity[I, O, C any](tst *Tester, act *flows.Activity[I, O, C], fn flows.InvokeFunc[I, O, C]) {
	tst.t.Helper()
	tst.setMock(act, (func(context.Context, flows.Key, I) (O, C, error))(fn), false)
}

// OnCompensator runs fn instead of act's compensator in this tester (a
// mocked activity otherwise compensates with the real one).
func OnCompensator[I, O, C any](tst *Tester, act *flows.Activity[I, O, C], fn flows.CompensateFunc[C]) {
	tst.t.Helper()
	tst.setMock(act, (func(context.Context, flows.Key, C) error)(fn), true)
}

func (tst *Tester) setMock(act, fn any, compensate bool) {
	tst.t.Helper()
	name, a, err := bridge.Mock(act, fn, compensate)
	if err != nil {
		tst.t.Fatalf("flowtest: %v", err)
	}
	tst.mu.Lock()
	tst.mocks[mockKey{name, compensate}] = a
	tst.mu.Unlock()
}

// Engine returns the tester's engine (Signal, Cancel, Handle, ...).
func (tst *Tester) Engine() *flows.Engine { return tst.e }

// Now returns the virtual time.
func (tst *Tester) Now() time.Time { return tst.clock.Now() }

// SignalAfter sends a signal d of virtual time from now (while Run or
// Advance moves time). It stays scheduled across Runs: a signal for an
// instance that has ended is reported as a test error when it is due.
func (tst *Tester) SignalAfter(d time.Duration, instanceID, name string, payload any) {
	tst.mu.Lock()
	defer tst.mu.Unlock()
	tst.signals = append(tst.signals, scheduledSignal{at: tst.clock.Now().Add(d), instance: instanceID, name: name, payload: payload})
	sort.SliceStable(tst.signals, func(i, j int) bool { return tst.signals[i].at.Before(tst.signals[j].at) })
}

// Run starts wf with instance id instanceID and drives the engine, moving
// virtual time, until the execution ends (or parks), then returns its
// result (Handle.Wait). ErrBlocked when it can never end.
func Run[I, O any](tst *Tester, wf *flows.Workflow[I, O], instanceID string, in I) (O, error) {
	tst.t.Helper()
	var zero O
	h, err := wf.Start(context.Background(), tst.e, instanceID, in)
	if err != nil {
		return zero, err
	}
	if err := tst.drive(func() bool {
		st, err := h.Status(context.Background())
		return err == nil && (!st.Active() || st == flows.StatusNeedsAttention)
	}); err != nil {
		return zero, fmt.Errorf("%s: %w", instanceID, err)
	}
	if !tst.cfg.allowPending {
		tst.reportPending(instanceID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.Wait(ctx)
}

// Settle drives the engine until everything that can run now has run,
// without moving virtual time.
func (tst *Tester) Settle() {
	tst.t.Helper()
	if err := tst.waitIdle(); err != nil {
		tst.t.Fatal(err)
	}
}

// Advance moves virtual time by d, running whatever becomes due on the
// way, in order.
func (tst *Tester) Advance(d time.Duration) {
	tst.t.Helper()
	end := tst.clock.Now().Add(d)
	for {
		tst.Settle()
		next, ok := tst.nextWake()
		if !ok || next.After(end) {
			break
		}
		tst.moveTo(next)
	}
	tst.moveTo(end)
	tst.Settle()
}

// ErrNeverIdle reports an engine that stayed busy for 30s of wall time:
// an activity running for long, or one waiting for a cancel that only a
// workflow timer would send (see the package documentation: virtual time
// does not move while activities run, so that timer never fires).
var ErrNeverIdle = errors.New("flowtest: the engine never became idle (an activity running for long, " +
	"or waiting for a cancel that a workflow timer would send: virtual time does not move while activities run; mock the activity)")

// idleTimeout is how long (wall time) waitIdle waits for an idle engine
// before ErrNeverIdle.
var idleTimeout = 30 * time.Second

// drive runs until done, moving virtual time when idle.
func (tst *Tester) drive(done func() bool) error {
	for {
		if err := tst.waitIdle(); err != nil {
			return err
		}
		if done() {
			return nil
		}
		next, ok := tst.nextWake()
		if !ok {
			return ErrBlocked
		}
		tst.moveTo(next)
	}
}

// nextWake is the earliest of the store's next visible task and the next
// scheduled signal.
func (tst *Tester) nextWake() (time.Time, bool) {
	_, next, ok := tst.st.Wake("flowtest", tst.queues)
	tst.mu.Lock()
	defer tst.mu.Unlock()
	if len(tst.signals) > 0 && (!ok || tst.signals[0].at.Before(next)) {
		return tst.signals[0].at, true
	}
	return next, ok
}

// moveTo sets the clock to at (if later) and sends the signals due.
func (tst *Tester) moveTo(at time.Time) {
	if d := at.Sub(tst.clock.Now()); d > 0 {
		tst.clock.Advance(d)
	}
	for {
		tst.mu.Lock()
		if len(tst.signals) == 0 || tst.signals[0].at.After(tst.clock.Now()) {
			tst.mu.Unlock()
			return
		}
		s := tst.signals[0]
		tst.signals = tst.signals[1:]
		tst.mu.Unlock()
		if err := tst.e.Signal(context.Background(), s.instance, s.name, s.payload); err != nil {
			tst.t.Errorf("flowtest: scheduled signal %s to %s: %v", s.name, s.instance, err)
		}
	}
}

// waitIdle waits (wall time) until the engine is idle with nothing
// claimable: a few consecutive observations, as pollers wake every 2ms.
func (tst *Tester) waitIdle() error {
	deadline := time.Now().Add(idleTimeout)
	for quiet := 0; quiet < 4; {
		ready, _, _ := tst.st.Wake("flowtest", tst.queues)
		if ready || tst.w.Busy() {
			quiet = 0
		} else {
			quiet++
		}
		if time.Now().After(deadline) {
			return ErrNeverIdle
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// reportPending reports, as test errors, the futures not awaited by the
// ended executions of the tree of instanceID that no Run reported yet:
// each ended execution is reported exactly once, at the first Run whose
// tree contains it. The tree is every execution of instanceID (whatever
// its parent) and every execution whose parent is in the tree; a child
// that never started (its id in use) has no execution there. Executions
// still running (or parked) are not checked.
func (tst *Tester) reportPending(instanceID string) {
	inTree := map[int64]bool{}
	var order []string               // instances with reports, first seen first
	pending := map[string][]string{} // by instance
	// Id order: an execution comes after its parent and after the earlier
	// executions of its chain.
	for _, ex := range tst.st.Executions() {
		if ex.InstanceID != instanceID && !inTree[ex.ParentID] {
			continue
		}
		inTree[ex.ID] = true
		if ex.Status.Active() {
			continue
		}
		tst.mu.Lock()
		done := tst.reported[ex.ID]
		if tst.reported == nil {
			tst.reported = map[int64]bool{}
		}
		tst.reported[ex.ID] = true
		d, dropped := tst.dropped[ex.ID]
		delete(tst.dropped, ex.ID)
		tst.mu.Unlock()
		if done {
			continue
		}
		p := tst.pendingFutures(ex.ID)
		if dropped {
			p = append(p, d)
		}
		if len(p) == 0 {
			continue
		}
		if _, ok := pending[ex.InstanceID]; !ok {
			order = append(order, ex.InstanceID)
		}
		pending[ex.InstanceID] = append(pending[ex.InstanceID], p...)
	}
	for _, id := range order {
		tst.t.Errorf("flowtest: %s ended with futures it did not await: %s", id, strings.Join(pending[id], ", "))
	}
}

// pendingFutures lists the activities and children an ended execution
// started and had no outcome for when it ended.
func (tst *Tester) pendingFutures(executionID int64) []string {
	hist, err := tst.st.LoadHistory(context.Background(), executionID, 0)
	if err != nil {
		return nil
	}
	open := map[int32]string{}
	var order []int32
	for _, ev := range hist {
		switch ev.Type {
		case history.ActivityScheduled:
			open[ev.ScheduleID] = "activity " + ev.Name + " (schedule id " + strconv.Itoa(int(ev.ScheduleID)) + ")"
			order = append(order, ev.ScheduleID)
		case history.ChildScheduled:
			open[ev.ScheduleID] = fmt.Sprintf("child %s %q (schedule id %d)", ev.Name, ev.Ref, ev.ScheduleID)
			order = append(order, ev.ScheduleID)
		case history.ActivityCompleted, history.ActivityFailed, history.ChildCompleted, history.ChildFailed:
			delete(open, ev.ScheduleID)
		default:
		}
	}
	var pending []string
	for _, id := range order {
		if s, ok := open[id]; ok {
			pending = append(pending, s)
		}
	}
	return pending
}

// EventTypes lists the event types of the history of the latest execution
// of instanceID (for assertions such as "the timer never fired").
func (tst *Tester) EventTypes(instanceID string) []string {
	ctx := context.Background()
	ex, err := tst.st.GetExecution(ctx, instanceID)
	if err != nil {
		return nil
	}
	hist, err := tst.st.LoadHistory(ctx, ex.ID, 0)
	if err != nil {
		return nil
	}
	out := make([]string, len(hist))
	for i, ev := range hist {
		out[i] = ev.Type.String()
	}
	return out
}
