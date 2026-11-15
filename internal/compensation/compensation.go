// Package compensation plans the compensation of a failed or canceled
// execution from its history alone: no workflow code runs, so recovery
// never needs the workflow function (saga semantics: compensators are found
// by activity name, with their persisted compensation data).
//
// The activities to undo are those that completed, and those that failed
// but may have had their effect (Uncertain), and that had a compensator:
// whether one existed is recorded with the outcome, by the binary that ran
// the activity (non-empty compensation data, "null" at least), never looked
// up in the planning binary's registry, which may be older or newer in a
// mixed deploy. They are undone one at a time, in the reverse order of
// their outcomes, once no activity is still running (its outcome decides).
// A compensation that fails for good parks the execution until a retry is
// requested.
package compensation

import (
	"slices"

	"github.com/imdat99/go-flows/internal/history"
)

// Action is what the engine does next.
type Action uint8

const (
	// Wait means a compensation, or an activity that may need one, is running.
	Wait Action = iota + 1
	// Schedule means: schedule the compensation of Step.
	Schedule
	// Park means the compensation of Step failed for good.
	Park
	// Finish means everything is compensated; Ran reports whether any
	// compensation ran at all.
	Finish
)

func (a Action) String() string {
	switch a {
	case Wait:
		return "wait"
	case Schedule:
		return "schedule"
	case Park:
		return "park"
	case Finish:
		return "finish"
	}
	return "invalid"
}

// Step is the compensation of one activity.
type Step struct {
	ScheduleID int32
	Name       string
	Queue      string
	Data       []byte // the activity's compensation data
	Uncertain  bool   // the activity may not have had its effect
	Err        string // Park: the compensation's error
	ErrKinds   string
}

type outcome uint8

const (
	running outcome = iota
	completed
	failedCertain
	failedUncertain
)

type compState uint8

const (
	none compState = iota
	open
	compensated
	compFailed
)

type activity struct {
	name, queue string
	data        []byte
	outcome     outcome
	comp        compState
	err, kinds  string // of a failed compensation
}

// Ledger is the compensation state of an execution, built by adding its
// history events in order.
type Ledger struct {
	acts     map[int32]*activity
	order    []int32 // compensable activities in outcome order
	timers   map[int32]bool
	children map[int32]string
	end      *history.Event
	failed   int32 // schedule id of a failed compensation awaiting retry (0: none)
	ran      bool  // a compensation was scheduled
}

// New returns an empty ledger.
func New() *Ledger {
	return &Ledger{acts: map[int32]*activity{}, timers: map[int32]bool{}, children: map[int32]string{}}
}

// Add applies one history event.
func (l *Ledger) Add(ev *history.Event) {
	switch ev.Type {
	case history.ActivityScheduled:
		l.acts[ev.ScheduleID] = &activity{name: ev.Name, queue: ev.Queue}
	case history.ActivityCompleted:
		l.outcome(ev, completed)
	case history.ActivityFailed:
		if ev.Uncertain {
			l.outcome(ev, failedUncertain)
		} else {
			l.outcome(ev, failedCertain)
		}
	case history.TimerScheduled:
		l.timers[ev.ScheduleID] = true
	case history.TimerFired, history.TimerCanceled:
		delete(l.timers, ev.ScheduleID)
	case history.ChildScheduled:
		l.children[ev.ScheduleID] = ev.Ref
	case history.ChildCompleted, history.ChildFailed:
		delete(l.children, ev.ScheduleID)
	case history.ExecutionFailed, history.ExecutionCanceled:
		end := *ev
		l.end = &end
	case history.CompensationScheduled:
		l.comp(ev.ScheduleID, open)
		l.ran = true
	case history.CompensationCompleted:
		l.comp(ev.ScheduleID, compensated)
	case history.CompensationFailed:
		l.comp(ev.ScheduleID, compFailed)
		if a := l.acts[ev.ScheduleID]; a != nil {
			a.err, a.kinds = ev.Err, ev.ErrKinds
		}
		l.failed = ev.ScheduleID
	case history.CompensationRetryRequested:
		if a := l.acts[l.failed]; a != nil && a.comp == compFailed {
			a.comp = none
		}
		l.failed = 0
	default:
	}
}

func (l *Ledger) outcome(ev *history.Event, o outcome) {
	a := l.acts[ev.ScheduleID]
	if a == nil || a.outcome != running {
		return
	}
	a.outcome = o
	if o != failedCertain && len(ev.CompData) > 0 { // it has a compensator
		a.data = ev.CompData
		l.order = append(l.order, ev.ScheduleID)
	}
}

func (l *Ledger) comp(id int32, s compState) {
	if a := l.acts[id]; a != nil {
		a.comp = s
	}
}

// End returns the event that ended the workflow (ExecutionFailed or
// ExecutionCanceled), or nil.
func (l *Ledger) End() *history.Event { return l.end }

// Ran reports whether any compensation was scheduled: a workflow whose
// compensation ran ends compensated, otherwise as it ended (failed).
func (l *Ledger) Ran() bool { return l.ran }

// Next returns the next action and the step it is about.
func (l *Ledger) Next() (Action, Step) {
	if l.failed != 0 {
		if a := l.acts[l.failed]; a != nil && a.comp == compFailed {
			return Park, l.step(l.failed, a)
		}
	}
	for _, a := range l.acts {
		if a.comp == open {
			return Wait, Step{}
		}
		if a.outcome == running {
			return Wait, Step{}
		}
	}
	for _, id := range slices.Backward(l.order) {
		a := l.acts[id]
		if a.comp == compensated {
			continue
		}
		return Schedule, l.step(id, a)
	}
	return Finish, Step{}
}

func (l *Ledger) step(id int32, a *activity) Step {
	return Step{
		ScheduleID: id, Name: a.name, Queue: a.queue, Data: a.data, Uncertain: a.outcome == failedUncertain,
		Err: a.err, ErrKinds: a.kinds,
	}
}

// Trivial reports whether there is nothing to compensate, nor to wait for:
// the execution can end at once.
func (l *Ledger) Trivial() bool {
	for _, a := range l.acts {
		if a.outcome == running || len(a.data) > 0 && a.comp != compensated {
			return false
		}
	}
	return true
}

// Running returns the schedule ids of activities without an outcome (to
// cancel when the workflow ends).
func (l *Ledger) Running() []int32 {
	var ids []int32
	for id, a := range l.acts {
		if a.outcome == running {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// Timers returns the schedule ids of timers that have not fired.
func (l *Ledger) Timers() []int32 {
	var ids []int32
	for id := range l.timers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Children returns the instance ids of children without an outcome.
func (l *Ledger) Children() []string {
	ids := make([]int32, 0, len(l.children))
	for id := range l.children {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	refs := make([]string, len(ids))
	for i, id := range ids {
		refs[i] = l.children[id]
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
}
