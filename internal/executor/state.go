package executor

import (
	"errors"
	"fmt"
	"slices"
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
	cancel    coro.CancelFunc
	queue     string          // the execution's queue (from ExecutionStarted)
	out       []history.Event // events of the current task
	end       *ending
	// checkedTo counts the commands checked at the last TaskStarted: the
	// ones issued since must all be matched by recorded events by the next
	// one (checkTaskBoundary).
	checkedTo int
	// committedTo counts the leading calls that are settled (done, canceled
	// or dropped with nothing left to commit): commit skips them.
	committedTo int
	// sides holds the recorded side effect values of the history being
	// replayed, by schedule id.
	sides map[int32][]byte
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
	cmd command.Command
	f   coro.Future[Result]
}

func (c *resultCall) command() *command.Command { return &c.cmd }

// OnCancel implements coro.Hook: the context of the call was canceled.
func (c *resultCall) OnCancel() {
	_ = c.cmd.Cancel() // activities and children are cancelable
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
	cmd command.Command
	f   coro.Future[struct{}]
}

func (c *timerCall) command() *command.Command { return &c.cmd }

// OnCancel implements coro.Hook.
func (c *timerCall) OnCancel() {
	_ = c.cmd.Cancel()
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
	st := &State{ex: e, signals: make(map[string]*coro.Channel[[]byte])}
	root, cancel := coro.WithCancelCause(coro.WithValue(e.sched.Root(), stateKey{}, st))
	st.root, st.cancel = root, func() { cancel(ErrCanceled) }
	return st
}

// Now returns the workflow time: the start of the current task (database
// time).
func (st *State) Now() time.Time { return st.now }

// Replaying reports whether the workflow is replaying recorded history
// (side effects such as logging should be skipped).
func (st *State) Replaying() bool { return st.replaying }

// Root returns the workflow's root context.
func (st *State) Root() coro.Context { return st.root }

func (st *State) cancelRoot() { st.cancel() }

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
	var can *ContinueAsNewError
	switch {
	case err == nil:
		st.end = &ending{
			status: store.StatusCompleted, output: out,
			event: history.Event{Type: history.ExecutionCompleted, Payload: out},
		}
	case errors.As(err, &can):
		wf := st.ex.wf
		st.end = &ending{
			status: store.StatusContinued,
			next:   &store.NewExecution{Workflow: wf.Name, Version: wf.Version, Queue: st.queue, Input: can.Input},
			event:  history.Event{Type: history.ExecutionContinuedAsNew, Payload: can.Input},
		}
	case errors.Is(err, ErrCanceled) && st.root.Err() != nil:
		st.end = &ending{
			status: store.StatusCanceled, err: err.Error(),
			event: history.Event{Type: history.ExecutionCanceled, Err: err.Error()},
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

func (st *State) issue(c call) {
	st.nextID++
	c.command().ID = st.nextID
	st.calls = append(st.calls, c)
}

// ScheduleActivity issues an activity; the future resolves with its result,
// or with ErrCanceled when ctx is canceled first.
func (st *State) ScheduleActivity(ctx coro.Context, name, queue string, input []byte) *coro.Future[Result] {
	c := &resultCall{cmd: command.Command{Kind: command.Activity, Name: name, Queue: queue, Payload: input, ArgHash: history.HashArgs(input)}}
	st.issue(c)
	coro.Watch(ctx, c)
	return &c.f
}

// StartChild issues a child workflow start; the future resolves with its
// outcome, or with ErrCanceled when ctx is canceled first (the child is
// asked to cancel).
func (st *State) StartChild(ctx coro.Context, instanceID, workflow string, version int32, queue string, input []byte) *coro.Future[Result] {
	c := &resultCall{cmd: command.Command{
		Kind: command.Child, Name: workflow, Version: version, Ref: instanceID, Queue: queue,
		Payload: input, ArgHash: history.HashArgs(input),
	}}
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
	c := &timerCall{cmd: command.Command{Kind: command.Timer, Name: name, At: st.now.Add(d).Truncate(time.Microsecond)}}
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

// checkTaskBoundary runs at each TaskStarted: every command issued by the
// previous task must have been matched by its recorded events (or dropped
// before commit). An unmatched one means this code issues commands the
// recorded run did not: non-determinism (GW-11).
func (st *State) checkTaskBoundary() error {
	for _, c := range st.calls[st.checkedTo:] {
		if cmd := c.command(); cmd.State == command.Pending {
			return fmt.Errorf("%w: %v %q (schedule id %d) issued on replay is not in the history",
				ErrNonDeterministic, cmd.Kind, cmd.Name, cmd.ID)
		}
	}
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
