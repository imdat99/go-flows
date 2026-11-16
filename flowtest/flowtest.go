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
// never come fails Run with ErrBlocked, at once. Run reports activities and
// children the workflow started but did not await (#436).
//
// Activities run for real unless mocked; their own sleeps take wall time,
// and virtual time does not move while one runs. So an activity that waits
// for its context to be canceled by a workflow timer (Select over a timer
// and the activity, canceling it) never sees the timer fire: mock it to
// return.
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
	// dropped: what workflows started in their last task without awaiting
	// it, by instance id (not in their histories: never started).
	dropped map[string]string
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
	w.SetDroppedHook(func(id string, acts, kids int) {
		tst.mu.Lock()
		defer tst.mu.Unlock()
		if tst.dropped == nil {
			tst.dropped = map[string]string{}
		}
		msg := fmt.Sprintf("%d activities and %d children started in the last task (not started)", acts, kids)
		if prev := tst.dropped[id]; prev != "" { // an earlier execution of the instance (continue-as-new)
			msg = prev + "; " + msg
		}
		tst.dropped[id] = msg
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
		pending := tst.pendingFutures(instanceID)
		tst.mu.Lock()
		if d, ok := tst.dropped[instanceID]; ok {
			pending = append(pending, d)
		}
		tst.mu.Unlock()
		if len(pending) > 0 {
			tst.t.Errorf("flowtest: %s ended with futures it did not await: %s", instanceID, strings.Join(pending, ", "))
		}
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

// ErrNeverIdle reports an engine that stayed busy for 30s of wall time.
var ErrNeverIdle = errors.New("flowtest: the engine never became idle (an activity running for long, " +
	"or waiting for a cancel that a workflow timer would send: virtual time does not move while activities run)")

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
	deadline := time.Now().Add(30 * time.Second)
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

// pendingFutures lists the activities and children an instance's latest
// execution started and had no outcome for when it ended.
func (tst *Tester) pendingFutures(instanceID string) []string {
	ctx := context.Background()
	ex, err := tst.st.GetExecution(ctx, instanceID)
	if err != nil || ex.Status.Active() {
		return nil
	}
	hist, err := tst.st.LoadHistory(ctx, ex.ID, 0)
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
	var out []string
	for _, id := range order {
		if s, ok := open[id]; ok {
			out = append(out, s)
		}
	}
	return out
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
