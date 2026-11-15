// Package executor runs a workflow function deterministically against its
// history: it replays recorded events, applies the events of a new task, and
// turns the commands the workflow issues into a store.Completion.
//
// Ported in structure from go-workflows workflow/executor (MIT, see NOTICE),
// rewritten on internal/coro and internal/command. Differences that matter:
//   - Replay matching compares type, name, argument hash, timer deadline and
//     child id of every command (command.Replay), and detects a replay that
//     issues fewer or more commands than recorded: commands issued by a
//     recorded task but missing from its recorded events, or recorded
//     events with no command (GW-11).
//   - Non-determinism is an error for the caller to park the execution
//     (needs_attention), never a workflow failure.
//   - A panic in workflow code fails the workflow with a stack; it never
//     reaches the worker (GW-10).
//   - Time is the TaskStarted time (database time); no wall clock.
//   - The workflow completes when its main function returns.
package executor

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/imdat99/go-flows/internal/command"
	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

var (
	// ErrNonDeterministic reports a replay that diverged from the history.
	// The execution must be parked, not failed.
	ErrNonDeterministic = command.ErrNonDeterministic
	// ErrCanceled is the error of an activity, timer or child whose context
	// was canceled.
	ErrCanceled = coro.ErrCanceled
	// ErrLockHeld is wrapped by the error of a lock request answered
	// LockHeld (*LockHeldError).
	ErrLockHeld = errors.New("lock held by another execution")
	// ErrLockLost ends a workflow whose business lock lapsed while it was
	// not running and was taken by another execution.
	ErrLockLost = errors.New("lock lost")
	// ErrContinueAsNew is returned (wrapped by ContinueAsNewError) by a
	// workflow that continues as new.
	ErrContinueAsNew = errors.New("continue as new")
	// errWrongWorkflow reports history of another workflow.
	errWrongWorkflow = errors.New("executor: history belongs to another workflow")
)

// Func is a workflow function on encoded input and output.
type Func func(ctx coro.Context, input []byte) ([]byte, error)

// Workflow identifies the function an executor runs.
type Workflow struct {
	Name    string
	Version int32
	Run     Func
}

// ContinueAsNewError ends the workflow and starts the next execution with
// Input.
type ContinueAsNewError struct{ Input []byte }

func (e *ContinueAsNewError) Error() string { return "continue as new" }

// Is matches ErrContinueAsNew.
func (e *ContinueAsNewError) Is(target error) bool { return target == ErrContinueAsNew }

// Options configure an executor.
type Options struct {
	// StuckTimeout bounds how long workflow code may run without yielding
	// (coro.DefaultStuckTimeout when 0).
	StuckTimeout time.Duration
	// ErrorText and ErrorKinds turn a workflow's error into its recorded
	// form (storage-safe text; comma-separated sentinel kinds). Defaults:
	// err.Error() and no kinds.
	ErrorText, ErrorKinds func(error) string
}

func (o *Options) errorText(err error) string {
	if o.ErrorText != nil {
		return o.ErrorText(err)
	}
	return err.Error()
}

func (o *Options) errorKinds(err error) string {
	if o.ErrorKinds != nil {
		return o.ErrorKinds(err)
	}
	return ""
}

// Executor holds the replayed state of one execution between its tasks
// (the sticky cache keeps it). Not safe for concurrent use.
type Executor struct {
	wf    Workflow
	opts  Options
	sched *coro.Scheduler
	st    *State

	executionID int64
	lastSeq     int64
	started     bool
	finished    bool  // a terminal event is recorded
	broken      error // set by the first error: the executor is unusable
	closed      bool
	// tail holds replayed events of a task that may not be complete yet
	// (history loaded in pages): a task is applied only once its next
	// TaskStarted, or Execute, shows it complete, because a replayed side
	// effect needs its recorded value when the workflow reaches it.
	tail []history.Event
	// Counts for MayCompensate: activities scheduled, their outcomes, and
	// the outcomes recorded with compensation data.
	activities, outcomes, compensable int
}

// errBroken wraps the first error for every later call.
var errBroken = errors.New("executor: unusable after an error")

// New returns an executor for one execution of wf.
func New(wf Workflow, executionID int64, opts Options) *Executor {
	e := &Executor{wf: wf, opts: opts, executionID: executionID, sched: coro.NewScheduler(opts.StuckTimeout)}
	e.st = newState(e)
	return e
}

// LastSeq is the sequence number of the last history event replayed or
// recorded (including events held until their task is complete).
func (e *Executor) LastSeq() int64 {
	if n := len(e.tail); n > 0 {
		return e.tail[n-1].Seq
	}
	return e.lastSeq
}

// count updates the MayCompensate counts with an applied event.
func (e *Executor) count(ev *history.Event) {
	switch ev.Type {
	case history.ActivityScheduled:
		e.activities++
	case history.ActivityCompleted, history.ActivityFailed:
		e.outcomes++
		if len(ev.CompData) > 0 {
			e.compensable++
		}
	default:
	}
}

// MayCompensate reports whether the history may hold something to
// compensate: an activity outcome with compensation data, or an activity
// without an outcome yet. When false, a failed workflow ends at once.
func (e *Executor) MayCompensate() bool {
	return e.compensable > 0 || e.activities > e.outcomes
}

// ExecutionID is the execution this executor runs.
func (e *Executor) ExecutionID() int64 { return e.executionID }

// Close stops the workflow's coroutines (their deferred functions run).
func (e *Executor) Close() {
	if !e.closed {
		e.closed = true
		e.sched.Close()
	}
}

// Replay applies recorded history events (continuing from LastSeq). It
// returns ErrNonDeterministic (wrapped) when the workflow diverges, or
// coro.ErrStuck; the executor is unusable after an error.
func (e *Executor) Replay(events []history.Event) (err error) {
	if e.broken != nil {
		return fmt.Errorf("%w: %w", errBroken, e.broken)
	}
	defer func() {
		if err != nil {
			e.broken = err
		}
	}()
	next := e.LastSeq() + 1
	for i := range events {
		if events[i].Seq != next+int64(i) {
			return fmt.Errorf("executor: history out of sequence: got %d, want %d", events[i].Seq, next+int64(i))
		}
	}
	if len(e.tail) > 0 {
		e.tail = append(e.tail, events...)
		events, e.tail = e.tail, nil
	}
	// Apply every complete task: those followed by a TaskStarted, or all of
	// it when the history ended (terminal event). Hold the rest, copied.
	cut := len(events)
	if n := len(events); n > 0 && !events[n-1].Type.IsTerminal() {
		cut = 0
		for i := n - 1; i > 0; i-- {
			if events[i].Type == history.TaskStarted {
				cut = i
				break
			}
		}
	}
	if err := e.applyBatch(events[:cut]); err != nil {
		return err
	}
	if cut < len(events) {
		e.tail = append(e.tail[:0:0], events[cut:]...)
	}
	return nil
}

// applyTail applies the held events.
func (e *Executor) applyTail() error {
	tail := e.tail
	e.tail = nil
	return e.applyBatch(tail)
}

// applyBatch applies events forming complete tasks.
func (e *Executor) applyBatch(batch []history.Event) error {
	if len(batch) == 0 {
		return nil
	}
	// Side effect values are known before the workflow asks for them, as in
	// the live run, where SideEffect returns at once: blocking until the
	// event is applied would let other coroutines take schedule ids first.
	e.st.recordedSides(batch)
	for i := range batch {
		ev := &batch[i]
		e.st.replaying = true
		if err := e.apply(ev); err != nil {
			return err
		}
		e.lastSeq = ev.Seq
	}
	e.st.compact()
	return nil
}

// Execute runs a workflow task: it records TaskStarted at task.Now, applies
// the delivered pending events in order, commits the commands the workflow
// issued, and returns the completion (Consumed set to every delivered
// event). The history must have been replayed up to task.LastSeq. Any
// error, including a task that does not continue the replayed history,
// makes the executor unusable: the caller evicts it.
func (e *Executor) Execute(task *store.WorkflowTask) (c *store.Completion, err error) {
	if e.broken != nil {
		return nil, fmt.Errorf("%w: %w", errBroken, e.broken)
	}
	defer func() {
		if err != nil {
			e.broken = err
		}
	}()
	if task.LastSeq != e.LastSeq() {
		return nil, fmt.Errorf("executor: task at seq %d, executor at %d", task.LastSeq, e.LastSeq())
	}
	if err := e.applyTail(); err != nil { // the history is complete
		return nil, err
	}
	st := e.st
	if err := st.endReplay(); err != nil {
		return nil, err
	}
	st.replaying = false
	st.out = st.out[:0]
	c = &store.Completion{}
	wasFinished := e.finished

	if !e.finished {
		if err := e.record(history.Event{Type: history.TaskStarted, At: task.Now}); err != nil {
			return nil, err
		}
	}
	for i := range task.Pending {
		p := &task.Pending[i]
		c.Consumed = append(c.Consumed, p.ID)
		if e.finished {
			continue // nothing more to deliver to (late outcome, signal after end)
		}
		if err := e.record(p.Event); err != nil {
			return nil, err
		}
	}
	if !e.finished {
		if err := e.commit(c); err != nil {
			return nil, err
		}
	}
	for i := range st.out {
		st.out[i].Seq = e.lastSeq + int64(i) + 1
	}
	c.History = append(c.History, st.out...)
	if n := len(st.out); n > 0 {
		e.lastSeq = st.out[n-1].Seq
	}
	if st.end != nil && !wasFinished { // this task recorded the end
		c.Status, c.Output, c.Err, c.ErrKinds = st.end.status, st.end.output, st.end.err, st.end.kinds
		c.ContinueAsNew = st.end.next
		if c.Status == store.StatusContinued {
			// GW-3: signals delivered to the workflow (this task included,
			// after it decided to continue) but not read go on, and so does
			// a cancel request it did not act on.
			c.Carry = st.unreadSignals()
			if st.root.Err() != nil {
				c.Carry = append(c.Carry, history.Event{Type: history.ExecutionCancelRequested})
			}
		}
	}
	st.trimSignals()
	return c, nil
}

// record appends a live event to the task's history and applies it.
func (e *Executor) record(ev history.Event) error {
	e.st.out = append(e.st.out, ev)
	return e.apply(&e.st.out[len(e.st.out)-1])
}

// commit appends the events of the commands issued in this task, fills the
// completion's side tables, and records the workflow's end.
func (e *Executor) commit(c *store.Completion) error {
	st := e.st
	if err := e.run(); err != nil {
		return err
	}
	if st.end != nil {
		// The workflow ended: commands still pending are dropped (an
		// activity started and not awaited is not run).
		for _, issued := range st.calls {
			if cmd := issued.command(); cmd.State == command.Pending {
				cmd.State = command.Dropped
			}
		}
	}
	for _, issued := range st.calls[st.committedTo:] {
		cmd := issued.command()
		before := len(st.out)
		var err error
		if st.out, err = cmd.Commit(st.out); err != nil {
			return err
		}
		for i := before; i < len(st.out); i++ {
			if st.out[i].Type == history.ActivityScheduled {
				e.activities++
			}
			sideTables(c, &st.out[i], cmd)
		}
	}
	st.compact()
	if st.end != nil {
		st.out = append(st.out, st.end.event)
		e.finished = true
	}
	return nil
}

// sideTables adds what the store must create for a committed event.
func sideTables(c *store.Completion, ev *history.Event, cmd *command.Command) {
	switch ev.Type {
	case history.ActivityScheduled:
		c.Activities = append(c.Activities, store.NewActivityTask{
			ScheduleID: ev.ScheduleID, Name: ev.Name, Queue: ev.Queue, Input: ev.Payload,
		})
	case history.TimerScheduled:
		c.Timers = append(c.Timers, history.Event{ScheduleID: ev.ScheduleID, Name: ev.Name, At: ev.At})
	case history.TimerCanceled:
		c.CanceledTimers = append(c.CanceledTimers, ev.ScheduleID)
	case history.ChildScheduled:
		c.Children = append(c.Children, store.NewExecution{
			InstanceID: cmd.Ref, Workflow: cmd.Name, Version: cmd.Version, Queue: cmd.Queue,
			Input: cmd.Payload, ParentSched: cmd.ID,
		})
	case history.ChildCancelRequested:
		c.ChildCancels = append(c.ChildCancels, cmd.Ref)
	case history.ActivityCancelRequested:
		c.CanceledActivities = append(c.CanceledActivities, cmd.ID)
	case history.LockRequested:
		us, _ := strconv.ParseInt(string(ev.Payload), 10, 64) // written by State.Lock
		c.Locks = append(c.Locks, store.LockRequest{ScheduleID: cmd.ID, Key: cmd.Name, TTL: time.Duration(us) * time.Microsecond})
	}
}

// run resumes the workflow's coroutines until all are blocked.
func (e *Executor) run() error {
	err := e.sched.Run()
	if err == nil {
		return nil
	}
	if errors.Is(err, coro.ErrStuck) || errors.Is(err, coro.ErrClosed) {
		return err
	}
	// A coroutine failed (a panic, or an error from the main function's
	// wrapper): the workflow ends with that error.
	e.st.finish(err)
	return nil
}

// apply applies one event (recorded or live) to the workflow state.
func (e *Executor) apply(ev *history.Event) error {
	st := e.st
	switch ev.Type {
	case history.TaskStarted:
		if err := st.checkTaskBoundary(); err != nil {
			return err
		}
		if st.end != nil && !e.finished {
			// The workflow returned, but the history goes on: the recorded
			// run ended later (or not yet).
			return fmt.Errorf("%w: the workflow ended on replay before the recorded end", ErrNonDeterministic)
		}
		st.now = ev.At
	case history.ExecutionStarted:
		if ev.Name != e.wf.Name || ev.Version != e.wf.Version {
			return fmt.Errorf("%w: %s@%d, executor runs %s@%d", errWrongWorkflow, ev.Name, ev.Version, e.wf.Name, e.wf.Version)
		}
		if e.started {
			return fmt.Errorf("%w: second ExecutionStarted", ErrNonDeterministic)
		}
		e.started = true
		st.startMain(ev.Queue, ev.Payload)
	case history.SignalReceived:
		_, _ = st.signal(ev.Name).TrySend(ev.Payload) // unbounded and never closed: cannot fail
		st.sigLog = append(st.sigLog, history.Event{Name: ev.Name, Payload: ev.Payload})
	case history.ExecutionCancelRequested:
		st.cancelRoot()
	case history.LockLost:
		// The section the lock protected was not exclusive: the workflow
		// fails (and compensates).
		st.finish(fmt.Errorf("%w: %q was taken by another execution while this one was not running", ErrLockLost, ev.Name))
	case history.ExecutionCompleted, history.ExecutionFailed, history.ExecutionContinuedAsNew,
		history.ExecutionCanceled:
		if st.end == nil || st.end.event.Type != ev.Type {
			return fmt.Errorf("%w: history ends with %v, the replayed workflow does not", ErrNonDeterministic, ev.Type)
		}
		e.finished = true
		return nil
	case history.ExecutionTerminated: // ended from outside: nothing to compare
		e.finished = true
		return nil
	default:
		e.count(ev)
		if err := st.applyCommandEvent(ev); err != nil {
			return err
		}
	}
	if e.finished {
		return nil
	}
	return e.run()
}
