package executor

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/imdat99/go-flows/internal/command"
	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Result is the outcome of an activity or child workflow.
type Result struct {
	Output []byte
	Err    error // a *RecordedError, or ErrCanceled
}

// RecordedError is an error as recorded in the history: its message, the
// sentinel kinds it matched, and whether it is permanent. The public API
// maps kinds back to sentinel errors.
type RecordedError struct {
	Message   string
	Kinds     []string
	Permanent bool
	Uncertain bool // the activity may have had its effect (see history)
}

func (e *RecordedError) Error() string { return e.Message }

// HasKind reports whether the error was recorded with kind.
func (e *RecordedError) HasKind(kind string) bool { return slices.Contains(e.Kinds, kind) }

func recordedError(ev *history.Event) *RecordedError {
	re := &RecordedError{Message: ev.Err, Permanent: ev.Permanent, Uncertain: ev.Uncertain}
	if ev.ErrKinds != "" {
		re.Kinds = strings.Split(ev.ErrKinds, ",")
	}
	return re
}

// State is the workflow-visible state of an execution, reached from the
// workflow's context (FromContext). All of it runs on the executor's
// coroutines, one at a time.
type State struct {
	ex        *Executor
	now       time.Time
	replaying bool
	nextID    int32
	calls     []call // issued commands, by schedule id - 1
	signals   map[string]*coro.Channel[[]byte]
	root      coro.Context
	cancel    coro.Canceler
	queue     string          // the execution's queue (from ExecutionStarted)
	out       []history.Event // events of the current task
	end       *ending
	// checkedTo counts the commands checked at the last TaskStarted: the
	// ones issued since must all be matched by recorded events by the next
	// one (checkTaskBoundary).
	checkedTo int
	// resultBlock is the rest of the block newResultCall takes from.
	resultBlock []resultCall
	// Initial storage of calls and out, in the state's own allocation.
	callsBuf [16]call
	outBuf   [8]history.Event
	// canceling holds the commands canceled since the last TaskStarted
	// that were committed already: their cancel must be matched by the next
	// one too (checkTaskBoundary).
	canceling []*command.Command
	// committedTo counts the leading calls that are settled (done, canceled
	// or dropped with nothing left to commit): commit skips them.
	committedTo int
	// sides holds the recorded side effect values of the history being
	// replayed, by schedule id.
	sides map[int32][]byte
	// sigLog lists the signals delivered to the channels, in arrival order,
	// while some may be unread: continue-as-new carries the unread ones.
	sigLog []history.Event
}

// unreadSignals returns the signals delivered but not received yet, in
// arrival order: each channel's unread values are its latest deliveries.
func (st *State) unreadSignals() []history.Event {
	left := make(map[string]int, len(st.signals))
	total := 0
	for name, ch := range st.signals {
		if n := ch.Len(); n > 0 {
			left[name] = n
			total += n
		}
	}
	if total == 0 {
		return nil
	}
	out := make([]history.Event, total)
	for i := len(st.sigLog) - 1; i >= 0 && total > 0; i-- {
		ev := st.sigLog[i]
		if left[ev.Name] > 0 {
			left[ev.Name]--
			total--
			out[total] = history.Event{Type: history.SignalReceived, Name: ev.Name, Payload: ev.Payload}
		}
	}
	return out
}

// trimSignals keeps the signal log to the unread deliveries (amortized: it
// compacts once the log is twice as long as what is unread).
func (st *State) trimSignals() {
	if len(st.sigLog) < 32 {
		return
	}
	unread := 0
	for _, ch := range st.signals {
		unread += ch.Len()
	}
	if len(st.sigLog) <= 2*unread {
		return
	}
	kept := st.unreadSignals()
	clear(st.sigLog)
	st.sigLog = append(st.sigLog[:0], kept...)
}

// call is an issued command with the future its outcome resolves. Each
// kind is one allocation: the command, the future and the cancel hook's
// registration live in the same struct.
type call interface {
	command() *command.Command
	// resolve applies the command's outcome event (ScheduleID matched).
	resolve(ev *history.Event) error
}

// resultCall is an activity or child workflow.
type resultCall struct {
	coro.HookSlot
	st  *State
	cmd command.Command
	f   coro.Future[Result]
}

func (c *resultCall) command() *command.Command { return &c.cmd }

// OnCancel implements coro.Hook: the context of the call was canceled.
func (c *resultCall) OnCancel() {
	_ = c.cmd.Cancel() // activities and children are cancelable
	c.st.canceled(&c.cmd)
	c.f.Set(Result{Err: ErrCanceled}, nil)
}

func (c *resultCall) resolve(ev *history.Event) error {
	coro.Unwatch(c)
	switch ev.Type {
	case history.ActivityCompleted, history.ChildCompleted:
		c.f.Set(Result{Output: ev.Payload}, nil)
	case history.ActivityFailed, history.ChildFailed:
		c.f.Set(Result{Err: recordedError(ev)}, nil)
	default:
		return fmt.Errorf("%w: %v for %v %q", ErrNonDeterministic, ev.Type, c.cmd.Kind, c.cmd.Name)
	}
	return nil
}

// timerCall is a timer.
type timerCall struct {
	coro.HookSlot
	st  *State
	cmd command.Command
	f   coro.Future[struct{}]
}

func (c *timerCall) command() *command.Command { return &c.cmd }

// OnCancel implements coro.Hook.
func (c *timerCall) OnCancel() {
	_ = c.cmd.Cancel()
	c.st.canceled(&c.cmd)
	c.f.Set(struct{}{}, ErrCanceled)
}

func (c *timerCall) resolve(ev *history.Event) error {
	coro.Unwatch(c)
	if ev.Type != history.TimerFired {
		return fmt.Errorf("%w: %v for a timer", ErrNonDeterministic, ev.Type)
	}
	c.f.Set(struct{}{}, nil)
	return nil
}

// lockCall is a lock request: the store answers it in the same checkpoint.
type lockCall struct {
	cmd command.Command
	f   coro.Future[struct{}]
}

func (c *lockCall) command() *command.Command { return &c.cmd }

func (c *lockCall) resolve(ev *history.Event) error {
	switch ev.Type {
	case history.LockAcquired:
		c.f.Set(struct{}{}, nil)
	case history.LockHeld:
		c.f.Set(struct{}{}, &LockHeldError{Key: c.cmd.Name, Holder: ev.Err})
	default:
		return fmt.Errorf("%w: %v for a lock", ErrNonDeterministic, ev.Type)
	}
	return nil
}

// LockHeldError is the error of a lock request answered LockHeld.
type LockHeldError struct{ Key, Holder string }

func (e *LockHeldError) Error() string {
	return "lock " + strconv.Quote(e.Key) + " held by " + e.Holder
}

// Unwrap makes errors.Is(err, ErrLockHeld) match.
func (e *LockHeldError) Unwrap() error { return ErrLockHeld }

// Lock asks for the business lock key for ttl (see store.Completion.Locks):
// nil once held, a *LockHeldError when another execution holds it. Not
// cancelable: the store answers in the checkpoint that records it.
//
// With wait > 0 the request waits (store.LockRequest.Wait, T9.8): a timer
// of wait named store.LockWaitTimer(key) is issued with it (one
// checkpoint: the store queues a refused request together with its timer,
// and a waiter whose timer is gone, canceled, has no turn). Granted, the
// timer is canceled; refused, Lock returns the *LockHeldError once the
// timer fired (early when the store wakes the waiter whose turn it is), or
// ctx's error when ctx ends first.
func (st *State) Lock(ctx coro.Context, key string, ttl, wait time.Duration) error {
	p := strconv.AppendInt(nil, ttl.Microseconds(), 10)
	c := &lockCall{cmd: command.Command{Kind: command.Lock, Name: key, Payload: p, ArgHash: history.HashArgs(p), Wait: wait}}
	st.issue(c)
	if wait <= 0 {
		_, err := c.f.Get(ctx)
		return err
	}
	tctx, cancel := coro.WithCancel(ctx)
	defer cancel() // the timer, unless it fired
	timer := st.NewTimer(tctx, wait, store.LockWaitTimer(key))
	_, err := c.f.Get(ctx)
	if err == nil || !errors.Is(err, ErrLockHeld) {
		return err
	}
	if _, terr := timer.Get(ctx); terr != nil {
		return terr
	}
	return err
}

// sideCall is a side effect: its value comes with its own event.
type sideCall struct {
	cmd command.Command
	f   coro.Future[[]byte]
}

func (c *sideCall) command() *command.Command { return &c.cmd }

func (c *sideCall) resolve(ev *history.Event) error {
	return fmt.Errorf("%w: %v for a side effect", ErrNonDeterministic, ev.Type)
}

type ending struct {
	event      history.Event
	status     store.Status
	output     []byte
	err, kinds string
	next       *store.NewExecution
}

type stateKey struct{}

// FromContext returns the state of the workflow ctx belongs to; nil when ctx
// is not a workflow context.
func FromContext(ctx coro.Context) *State {
	st, _ := ctx.Value(stateKey{}).(*State)
	return st
}

func newState(e *Executor) *State {
	// Most executions issue a few calls and events per task: grow once.
	// The signals map is made by the first signal channel.
	st := &State{ex: e}
	st.calls, st.out = st.callsBuf[:0], st.outBuf[:0]
	st.root, st.cancel = coro.WithCanceler(coro.WithValue(e.sched.Root(), stateKey{}, st))
	return st
}

// Now returns the workflow time: the start of the current task (database
// time).
func (st *State) Now() time.Time { return st.now }

// Replaying reports whether the workflow is replaying recorded history
// (side effects such as logging should be skipped).
func (st *State) Replaying() bool { return st.replaying }

// InstanceID returns the instance id of the execution (Options.WorkflowInstanceID).
func (st *State) InstanceID() string { return st.ex.opts.WorkflowInstanceID }

// ExecutionID returns the store's id of the execution.
func (st *State) ExecutionID() int64 { return st.ex.executionID }

// Root returns the workflow's root context.
func (st *State) Root() coro.Context { return st.root }

func (st *State) cancelRoot() { st.cancel.Cancel(ErrCanceled) }

func (st *State) startMain(queue string, input []byte) {
	st.queue = queue
	wf := st.ex.wf
	st.ex.sched.Spawn(st.root, func(ctx coro.Context) error {
		out, err := wf.Run(ctx, input)
		st.finishWith(out, err)
		return nil
	})
}

// finish ends the workflow with an error raised outside the main function
// (a panic in a coroutine).
func (st *State) finish(err error) {
	if st.end == nil {
		st.finishWith(nil, err)
	}
}

func (st *State) finishWith(out []byte, err error) {
	if st.end != nil {
		return
	}
	switch can := continueAsNew(err); {
	case err == nil:
		st.end = &ending{
			status: store.StatusCompleted, output: out,
			event: history.Event{Type: history.ExecutionCompleted, Payload: out},
		}
	case can != nil:
		wf := st.ex.wf
		st.end = &ending{
			status: store.StatusContinued,
			next:   &store.NewExecution{Workflow: wf.Name, Version: wf.Version, Queue: st.queue, Input: can.Input},
			event:  history.Event{Type: history.ExecutionContinuedAsNew, Payload: can.Input},
		}
	case errors.Is(err, ErrCanceled) && st.root.Err() != nil:
		// Stored like a failure: sanitized text (a NUL or invalid UTF-8
		// would be refused by the store forever), bounded, with its kinds.
		msg, kinds := st.ex.opts.errorText(err), st.ex.opts.errorKinds(err)
		st.end = &ending{
			status: store.StatusCanceled, err: msg, kinds: kinds,
			event: history.Event{Type: history.ExecutionCanceled, Err: msg, ErrKinds: kinds},
		}
	default:
		msg, kinds := st.ex.opts.errorText(err), st.ex.opts.errorKinds(err)
		st.end = &ending{
			status: store.StatusFailed, err: msg, kinds: kinds,
			event: history.Event{Type: history.ExecutionFailed, Err: msg, ErrKinds: kinds},
		}
	}
}

func (st *State) signal(name string) *coro.Channel[[]byte] {
	if st.signals == nil {
		st.signals = make(map[string]*coro.Channel[[]byte])
	}
	ch, ok := st.signals[name]
	if !ok {
		ch = coro.NewChannel[[]byte](-1) // never drop a signal (GW-14)
		st.signals[name] = ch
	}
	return ch
}

// SignalChannel returns the channel of signals called name, in arrival
// order.
func (st *State) SignalChannel(name string) *coro.Channel[[]byte] { return st.signal(name) }

// newResultCall returns a zero resultCall from the execution's current
// block (calls are allocated eight at a time; they live as long as the
// execution's state).
func (st *State) newResultCall() *resultCall {
	if len(st.resultBlock) == 0 {
		n := 16 // the first block: most executions need no other
		if len(st.calls) > 0 {
			n = 8
		}
		st.resultBlock = make([]resultCall, n)
	}
	c := &st.resultBlock[0]
	st.resultBlock = st.resultBlock[1:]
	return c
}

func (st *State) issue(c call) {
	st.nextID++
	c.command().ID = st.nextID
	st.calls = append(st.calls, c)
}

// ScheduleActivity issues an activity; the future resolves with its result,
// or with ErrCanceled when ctx is canceled first.
func (st *State) ScheduleActivity(ctx coro.Context, name, queue string, input []byte) *coro.Future[Result] {
	c := st.newResultCall()
	c.st, c.cmd = st, command.Command{Kind: command.Activity, Name: name, Queue: queue, Payload: input, ArgHash: history.HashArgs(input)}
	st.issue(c)
	coro.Watch(ctx, c)
	return &c.f
}

// StartChild issues a child workflow start; the future resolves with its
// outcome, or with ErrCanceled when ctx is canceled first (the child is
// asked to cancel).
func (st *State) StartChild(ctx coro.Context, instanceID, workflow string, version int32, queue string, input []byte) *coro.Future[Result] {
	c := st.newResultCall()
	c.st, c.cmd = st, command.Command{
		Kind: command.Child, Name: workflow, Version: version, Ref: instanceID, Queue: queue,
		Payload: input, ArgHash: history.HashArgs(input),
	}
	st.issue(c)
	coro.Watch(ctx, c)
	return &c.f
}

// NewTimer issues a timer firing d after the current workflow time. The
// future resolves when it fires, or with ErrCanceled when ctx is canceled
// first. d <= 0 resolves at once without a command.
func (st *State) NewTimer(ctx coro.Context, d time.Duration, name string) *coro.Future[struct{}] {
	if d <= 0 {
		f := coro.NewFuture[struct{}]()
		f.Set(struct{}{}, nil)
		return f
	}
	c := &timerCall{st: st, cmd: command.Command{Kind: command.Timer, Name: name, At: st.now.Add(d).Truncate(time.Microsecond)}}
	st.issue(c)
	coro.Watch(ctx, c)
	return &c.f
}

// SideEffect returns fn's value, recorded: fn runs once, live; replay
// returns the recorded value at once, as the live run did. Only a side
// effect the history lacks blocks (the task boundary check then reports the
// divergence).
func (st *State) SideEffect(ctx coro.Context, fn func() []byte) ([]byte, error) {
	c := &sideCall{cmd: command.Command{Kind: command.SideEffect}}
	st.issue(c)
	if !st.replaying {
		c.cmd.Payload = fn()
		c.f.Set(c.cmd.Payload, nil)
	} else if v, ok := st.sides[c.cmd.ID]; ok {
		c.f.Set(v, nil) // the recorded value, at the same point as the live run
	}
	// Not recorded: it blocks; the task boundary check reports the
	// divergence.
	return c.f.Get(ctx)
}

// applyCommandEvent applies an event tied to a command by schedule id.
func (st *State) applyCommandEvent(ev *history.Event) error {
	if ev.ScheduleID < 1 || int(ev.ScheduleID) > len(st.calls) {
		return fmt.Errorf("%w: %v for schedule id %d, but the workflow issued %d commands",
			ErrNonDeterministic, ev.Type, ev.ScheduleID, len(st.calls))
	}
	c := st.calls[ev.ScheduleID-1]
	cmd := c.command()
	if ev.Type.IsCommand() {
		if err := cmd.Replay(ev); err != nil {
			return err
		}
		if sc, ok := c.(*sideCall); ok {
			sc.f.Set(ev.Payload, nil)
			delete(st.sides, cmd.ID)
		}
		return nil
	}
	if err := cmd.Finish(); err != nil {
		return err
	}
	return c.resolve(ev)
}

// canceled notes a cancel of a committed command (checkTaskBoundary).
func (st *State) canceled(cmd *command.Command) {
	if cmd.State == command.CancelPending {
		st.canceling = append(st.canceling, cmd)
	}
}

// checkTaskBoundary runs at each TaskStarted: every command issued by the
// previous task must have been matched by its recorded events (or dropped
// before commit). An unmatched one means this code issues commands the
// recorded run did not: non-determinism (GW-11).
func (st *State) checkTaskBoundary() error {
	for _, c := range st.calls[st.checkedTo:] {
		switch cmd := c.command(); cmd.State {
		case command.Pending:
			return fmt.Errorf("%w: %v %q (schedule id %d) issued on replay is not in the history",
				ErrNonDeterministic, cmd.Kind, cmd.Name, cmd.ID)
		case command.Committed, command.CancelPending, command.Canceled, command.Dropped, command.Done:
		}
	}
	// A cancel is committed in the task that issues it: on replay, its
	// event (or the outcome delivered in that task) settles it before the
	// next TaskStarted. One still pending was not in the recorded run.
	for i, cmd := range st.canceling {
		st.canceling[i] = nil
		if cmd.State == command.CancelPending {
			st.canceling = st.canceling[:0]
			return fmt.Errorf("%w: cancel of %v %q (schedule id %d) issued on replay is not in the history",
				ErrNonDeterministic, cmd.Kind, cmd.Name, cmd.ID)
		}
	}
	st.canceling = st.canceling[:0]
	st.checkedTo = len(st.calls)
	return nil
}

// endReplay leaves replay mode before a live task. Commands the last
// replayed task issued but the history lacks are caught by the live
// TaskStarted's checkTaskBoundary.
func (st *State) endReplay() error {
	st.replaying = false
	return nil
}

func (st *State) recordedSides(events []history.Event) {
	for i := range events {
		if ev := &events[i]; ev.Type == history.SideEffectResult {
			if st.sides == nil {
				st.sides = make(map[int32][]byte)
			}
			st.sides[ev.ScheduleID] = ev.Payload
		}
	}
}

// compact releases what settled commands no longer need: their payload
// (replay matching uses the argument hash) and, for the leading run of
// settled commands, the commit scan.
func (st *State) compact() {
	for _, c := range st.calls[st.committedTo:] {
		cmd := c.command()
		settled := cmd.State == command.Done || cmd.State == command.Dropped || cmd.State == command.Canceled
		if !settled {
			break
		}
		cmd.Payload = nil
		st.committedTo++
	}
}

// continueAsNew returns the ContinueAsNewError in err's chain, or nil
// (errors.AsType allocates no target, unlike errors.As).
func continueAsNew(err error) *ContinueAsNewError {
	c, _ := errors.AsType[*ContinueAsNewError](err)
	return c
}
