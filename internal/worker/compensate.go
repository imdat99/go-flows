package worker

import (
	"errors"
	"fmt"
	"slices"

	"github.com/imdat99/go-flows/internal/compensation"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Compensation (saga layer). When a workflow fails or is canceled, the
// activities it completed (and those that may have had their effect) are
// undone by their compensators, one at a time in reverse order, planned
// from the history alone: no workflow code runs, so a compensating
// execution never needs its workflow function. The execution is
// compensating meanwhile, then compensated (or canceled); a compensation
// that fails for good parks it as needs_attention until
// store.ReopenCompensation.

// errHistoryGap reports a loaded history that does not end where the task
// says (an executor bug, or a concurrent writer: the lease fences that).
var errHistoryGap = errors.New("worker: history does not end at the task's sequence")

// beginCompensation turns the completion of a workflow that just failed or
// was canceled into the start of its compensation, unless there is nothing
// to compensate (then the execution ends as the workflow did).
func (d *driver) beginCompensation(task *store.WorkflowTask, c *store.Completion) error {
	hist, err := d.w.st.LoadHistory(d.ctx, task.Execution.ID, 0)
	if err != nil {
		return err
	}
	if n := len(hist); n > 0 && hist[n-1].Seq != task.LastSeq || n == 0 && task.LastSeq != 0 {
		return errHistoryGap
	}
	l := compensation.New()
	for i := range hist {
		l.Add(&hist[i])
	}
	for i := range c.History {
		l.Add(&c.History[i])
	}
	if l.Trivial() {
		return nil
	}
	// Stop what still runs: activities (their outcomes are still awaited
	// when they may need compensation), timers, children.
	for _, id := range l.Running() {
		if !slices.Contains(c.CanceledActivities, id) {
			c.CanceledActivities = append(c.CanceledActivities, id)
		}
	}
	c.CanceledTimers = append(c.CanceledTimers, l.Timers()...)
	c.ChildCancels = append(c.ChildCancels, l.Children()...)
	// The workflow's error is kept on the execution while it compensates
	// (for operators); the end records it again.
	c.Status, c.Output = store.StatusCompensating, nil
	d.plan(l, c, lastSeq(task, c))
	return nil
}

// compensate runs a workflow task of a compensating execution: it records
// the outcomes delivered, then plans the next step.
func (d *driver) compensate(task *store.WorkflowTask) (*store.Completion, error) {
	l := d.ledger
	if l == nil || d.ledgerSeq != task.LastSeq {
		hist, err := d.w.st.LoadHistory(d.ctx, task.Execution.ID, 0)
		if err != nil {
			return nil, err
		}
		if n := len(hist); n == 0 || hist[n-1].Seq != task.LastSeq {
			return nil, errHistoryGap
		}
		l = compensation.New()
		for i := range hist {
			l.Add(&hist[i])
		}
	}
	d.ledger = nil // until the completion is stored
	c := &store.Completion{}
	seq := task.LastSeq
	for i := range task.Pending {
		p := &task.Pending[i]
		c.Consumed = append(c.Consumed, p.ID)
		switch p.Event.Type {
		case history.ActivityCompleted, history.ActivityFailed, history.ChildCompleted, history.ChildFailed,
			history.CompensationCompleted, history.CompensationFailed, history.CompensationRetryRequested,
			history.ExecutionCompensationRequested:
			seq++
			ev := p.Event
			ev.Seq = seq
			c.History = append(c.History, ev)
			l.Add(&c.History[len(c.History)-1])
		default:
			// Signals, cancel requests and timers no longer matter: the
			// workflow has ended.
		}
	}
	if l.End() == nil {
		return nil, fmt.Errorf("worker: execution %d is compensating but its workflow has not ended", task.Execution.ID)
	}
	d.plan(l, c, seq)
	return c, nil
}

// plan appends to c the next steps of the compensation (records skipped
// ones, schedules the next compensation, or ends or parks the execution),
// and keeps the ledger for the next task of this driver.
func (d *driver) plan(l *compensation.Ledger, c *store.Completion, seq int64) {
	add := func(ev history.Event) {
		seq++
		ev.Seq = seq
		c.History = append(c.History, ev)
		l.Add(&c.History[len(c.History)-1])
	}
	action, s := l.Next()
	switch action {
	case compensation.Schedule:
		if s.Child {
			add(history.Event{Type: history.CompensationScheduled, ScheduleID: s.ScheduleID, Name: s.Name, Ref: s.Ref})
			c.CompensateChildren = append(c.CompensateChildren, s.ScheduleID)
			break
		}
		add(history.Event{
			Type: history.CompensationScheduled, ScheduleID: s.ScheduleID, Name: s.Name, Queue: s.Queue,
			Payload: s.Data, Uncertain: s.Uncertain,
		})
		c.Activities = append(c.Activities, store.NewActivityTask{
			ScheduleID: s.ScheduleID, Name: s.Name, Queue: s.Queue, Input: s.Data,
			Compensate: true, Uncertain: s.Uncertain,
		})
	case compensation.Park:
		c.Status = store.StatusNeedsAttention
		c.Err = "compensation of " + s.Name + " failed: " + s.Err
		c.ErrKinds = store.KindCompensationFailed
		if s.ErrKinds != "" {
			c.ErrKinds += "," + s.ErrKinds
		}
	case compensation.Finish:
		// Compensated if any compensation ran; otherwise (the activities
		// still running when the workflow ended had no compensator) it
		// ends as the workflow did.
		end := l.End()
		c.Status, c.Err, c.ErrKinds = store.StatusFailed, end.Err, end.ErrKinds
		switch {
		case end.Type == history.ExecutionCanceled:
			c.Status = store.StatusCanceled
		case l.Ran() || end.Type == history.ExecutionCompensationRequested:
			c.Status = store.StatusCompensated
		}
	case compensation.Wait:
	}
	if c.Status == "" {
		// Still compensating: keep the workflow's error on the execution
		// (a retried park left the compensation's reason there).
		end := l.End()
		c.Status, c.Err, c.ErrKinds = store.StatusCompensating, end.Err, end.ErrKinds
	}
	if c.Status == "" || c.Status == store.StatusCompensating {
		d.ledger, d.ledgerSeq = l, seq
	}
}

func lastSeq(task *store.WorkflowTask, c *store.Completion) int64 {
	if n := len(c.History); n > 0 {
		return c.History[n-1].Seq
	}
	return task.LastSeq
}
