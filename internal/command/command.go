// Package command holds the state machine of the commands a workflow issues
// (schedule an activity, a timer, a child workflow, record a side effect) and
// turns them into history events.
//
// Ported from go-workflows internal/command (MIT, see NOTICE). Changes:
//   - one concrete Command type instead of an interface per kind;
//   - events are appended to a caller's slice, so there is no result
//     allocation per command;
//   - invalid transitions return errors instead of panicking (GW-10);
//   - replay matching compares arguments, timer deadlines and child ids, not
//     only names (GW-11).
//
// The executor derives the rest from the scheduling events Commit returns:
//   - activity tasks (Name, Queue; policies come from the registry by Name);
//   - the pending TimerFired (visible at At);
//   - child instances (Ref, Queue, Payload).
//
// For a side effect, the value is the command's Payload when first executed
// and the recorded event's Payload during replay.
package command

import (
	"errors"
	"fmt"
	"time"

	"github.com/imdat99/go-flows/internal/history"
)

// Kind is what a command does.
type Kind uint8

const (
	// Activity schedules an activity.
	Activity Kind = iota + 1
	// Timer schedules a timer.
	Timer
	// Child starts a child workflow.
	Child
	// SideEffect records a value computed by the workflow.
	SideEffect
	// Lock asks for a business lock (Name; Payload is its TTL).
	Lock
)

func (k Kind) String() string {
	switch k {
	case Activity:
		return "activity"
	case Timer:
		return "timer"
	case Child:
		return "child workflow"
	case SideEffect:
		return "side effect"
	case Lock:
		return "lock"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

func (k Kind) valid() bool { return k >= Activity && k <= Lock }

// State of a command.
type State uint8

const (
	// Pending means issued by the workflow in this task, not yet committed.
	Pending State = iota
	// Committed means its scheduling event is written (or will be at the end
	// of this task, or was matched during replay).
	Committed
	// CancelPending means canceled by the workflow after being committed;
	// the cancel event is not written yet.
	CancelPending
	// Canceled means the cancel event is written: an outcome may still
	// arrive (an activity that finished before it saw the cancel).
	Canceled
	// Dropped means canceled before being committed: nothing was or will be
	// written, and no outcome can arrive.
	Dropped
	// Done means the outcome arrived (side effects are done when committed).
	Done
)

func (s State) String() string {
	switch s {
	case Pending:
		return "pending"
	case Committed:
		return "committed"
	case CancelPending:
		return "cancel-pending"
	case Canceled:
		return "canceled"
	case Dropped:
		return "dropped"
	case Done:
		return "done"
	}
	return fmt.Sprintf("State(%d)", uint8(s))
}

var (
	// ErrInvalidTransition is an internal error: a command was driven out of
	// order (corrupt history or an executor bug).
	ErrInvalidTransition = errors.New("command: invalid state transition")
	// ErrNonDeterministic reports a replay that issued a different command
	// than the one recorded at the same position.
	ErrNonDeterministic = errors.New("non-deterministic workflow")
)

// Command is one command of a workflow execution.
type Command struct {
	ID    int32 // schedule id, assigned in issue order
	Kind  Kind
	State State

	Name    string    // activity, workflow or timer name
	Version int32     // child workflow version
	Ref     string    // child instance id
	Queue   string    // activity or child queue
	Payload []byte    // encoded input (or the side effect value)
	ArgHash uint64    // history.HashArgs(Payload) for activities and children
	At      time.Time // timer deadline: task start (database time) + duration
}

func (c *Command) transition(to State) error {
	return fmt.Errorf("%w: %v %d %q: %v -> %v", ErrInvalidTransition, c.Kind, c.ID, c.Name, c.State, to)
}

// Commit appends the events of a pending command (or of a pending cancel) to
// events and advances the state. A command with nothing to write appends
// nothing.
func (c *Command) Commit(events []history.Event) ([]history.Event, error) {
	if !c.Kind.valid() {
		return events, c.transition(Committed)
	}
	switch c.State {
	case Pending:
		c.State = Committed
		c.At = c.At.Truncate(time.Microsecond) // the stored value is what counts
		if c.Kind == SideEffect {
			c.State = Done // no outcome event follows a side effect
		}
		return append(events, c.scheduledEvent()), nil
	case CancelPending:
		c.State = Canceled
		return append(events, history.Event{Type: c.cancelType(), ScheduleID: c.ID}), nil
	case Committed, Canceled, Dropped, Done:
		return events, nil
	}
	return events, c.transition(Committed)
}

func (c *Command) scheduledEvent() history.Event {
	ev := history.Event{ScheduleID: c.ID, Name: c.Name}
	switch c.Kind {
	case Activity:
		ev.Type = history.ActivityScheduled
		ev.Queue, ev.Payload, ev.ArgHash = c.Queue, c.Payload, c.ArgHash
	case Timer:
		ev.Type = history.TimerScheduled
		ev.At = c.At.Truncate(time.Microsecond) // database precision
	case Child:
		ev.Type = history.ChildScheduled
		ev.Version, ev.Ref, ev.Queue, ev.Payload, ev.ArgHash = c.Version, c.Ref, c.Queue, c.Payload, c.ArgHash
	case SideEffect:
		ev.Type = history.SideEffectResult
		ev.Payload = c.Payload
	case Lock:
		ev.Type = history.LockRequested
		ev.Payload, ev.ArgHash = c.Payload, c.ArgHash
	}
	return ev
}

func (c *Command) cancelType() history.Type {
	switch c.Kind {
	case Activity:
		return history.ActivityCancelRequested
	case Timer:
		return history.TimerCanceled
	case Child:
		return history.ChildCancelRequested
	case SideEffect, Lock:
	}
	return 0
}

// Cancelable reports whether the kind can be canceled.
func (c *Command) Cancelable() bool { return c.Kind != SideEffect && c.Kind != Lock && c.Kind.valid() }

// Cancel cancels the command:
//   - a pending command is dropped: nothing is written and no outcome
//     follows;
//   - a committed one gets a cancel event at the next Commit;
//   - canceling a command that is already canceled, dropped or done does
//     nothing.
func (c *Command) Cancel() error {
	if !c.Cancelable() {
		return c.transition(Canceled)
	}
	switch c.State {
	case Pending:
		c.State = Dropped
	case Committed:
		c.State = CancelPending
	case CancelPending, Canceled, Dropped, Done:
	}
	return nil
}

// Finish records the command's outcome.
//
// An outcome can arrive after a cancel. From Canceled this is an activity
// that completed before it saw the cancel. From CancelPending the outcome
// arrived in the same task as the cancel: the cancel event is then never
// written, and the original run and the replay agree on this because both
// see the outcome first.
func (c *Command) Finish() error {
	switch c.State {
	case Committed, CancelPending, Canceled:
		c.State = Done
		return nil
	case Pending, Dropped, Done:
	}
	return c.transition(Done)
}

// Replay matches the command against the event recorded at its position
// during replay, and advances the state as Commit would without producing
// events. A mismatch is ErrNonDeterministic with both sides described.
func (c *Command) Replay(ev *history.Event) error {
	if !c.Kind.valid() {
		return c.transition(Committed)
	}
	if ev.ScheduleID != c.ID {
		return c.mismatch(ev, "schedule id")
	}
	switch c.State {
	case Pending:
		if err := c.match(ev); err != nil {
			return err
		}
		c.State = Committed
		if c.Kind == SideEffect {
			c.State = Done
		}
		return nil
	case CancelPending:
		if ev.Type != c.cancelType() {
			return c.mismatch(ev, "type")
		}
		c.State = Canceled
		return nil
	case Committed, Canceled, Dropped, Done:
	}
	return c.mismatch(ev, "state "+c.State.String())
}

func (c *Command) match(ev *history.Event) error {
	want := c.scheduledEvent()
	switch {
	case ev.Type != want.Type:
		return c.mismatch(ev, "type")
	case ev.Name != want.Name:
		return c.mismatch(ev, "name")
	case ev.ArgHash != want.ArgHash:
		return c.mismatch(ev, "arguments")
	case c.Kind == Timer && !ev.At.Equal(want.At):
		return c.mismatch(ev, "timer deadline")
	case c.Kind == Child && (ev.Ref != want.Ref || ev.Version != want.Version):
		return c.mismatch(ev, "child workflow")
	}
	return nil
}

func (c *Command) mismatch(ev *history.Event, what string) error {
	return fmt.Errorf("%w: %s differs at schedule id %d: history has %v %q (id %d), workflow issued %v %q",
		ErrNonDeterministic, what, c.ID, ev.Type, ev.Name, ev.ScheduleID, c.Kind, c.Name)
}
