package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
)

// A driver runs one claimed execution for as long as this worker holds its
// lease: the claimed task, then, with eager execution, the tasks resumed by
// the results of the activities it runs itself. It is the only goroutine
// that touches the execution's executor and lease, so resumed tasks are
// processed one at a time.
//
// The eager loop, per activity:
//  1. Complete the workflow task, creating the activity task already
//     claimed and keeping the workflow lease (one round trip).
//  2. Run the activity in this process.
//  3. Complete the activity and resume the workflow, getting its next task
//     (one round trip).
//
// Activities run concurrently and send their outcome to the driver, which
// records them in arrival order. When they take longer than EagerWindow, the
// driver hands the execution over (releases the lease, caches the executor)
// and a detached recorder records the remaining outcomes, so the driver's
// workflow slot is free again.
type driver struct {
	w        *Worker
	ctx      context.Context // canceled when the workflow lease is lost or at the stop deadline
	lease    store.Lease
	held     bool   // the workflow lease is held (kept between tasks)
	untrack  func() // stops tracking the workflow lease in the heartbeat
	ex       *executor.Executor
	inflight int
	results  chan eagerResult
	running  map[int32]int64 // schedule id -> activity lease id of eager activities in flight
	window   *time.Timer     // reused for every wait on eager results
}

type eagerResult struct {
	task    store.ActivityTask
	outcome attemptOutcome
	untrack func()
}

// runWorkflowTask runs a claimed workflow task and everything it leads to.
func (w *Worker) runWorkflowTask(pollCtx context.Context, task *store.WorkflowTask, claimed time.Time) {
	tctx, untrack := w.hb.track(w.taskCtx(), false, task.Lease, claimed)
	d := &driver{
		w: w, ctx: tctx, lease: task.Lease, held: true, untrack: untrack,
		// Buffered for every activity that can run at once: an activity
		// never blocks on a driver that stopped reading (panic, hand-over).
		results: make(chan eagerResult, w.opts.MaxActivities),
		running: map[int32]int64{},
	}
	defer func() {
		if r := recover(); r != nil {
			w.log.LogAttrs(pollCtx, slog.LevelError, "flows: panic in workflow task", slog.Any("panic", r))
			d.evict()
			d.release(w.opts.HandBack)
			d.detach()
		}
		d.untrack()
	}()
	d.run(pollCtx, task)
}

// windowTimer (re)arms the eager window timer.
func (d *driver) windowTimer() <-chan time.Time {
	if d.window == nil {
		d.window = time.NewTimer(d.w.opts.EagerWindow)
	} else {
		d.window.Reset(d.w.opts.EagerWindow) // Go 1.23+ timers: no stale value to drain
	}
	return d.window.C
}

func (d *driver) run(pollCtx context.Context, task *store.WorkflowTask) {
	defer func() {
		if d.window != nil {
			d.window.Stop()
		}
		if d.ex != nil {
			d.ex.Close()
			d.ex = nil
		}
	}()
	for task != nil {
		if next := d.step(pollCtx, task); next != nil {
			task = next
			continue
		}
		task = nil
		var window <-chan time.Time
		if d.inflight > 0 && d.held {
			window = d.windowTimer()
		}
		for d.inflight > 0 && d.held && task == nil {
			select {
			case r := <-d.results:
				task = d.recordResult(r, true)
			case <-window:
				// The activities take long: stop holding the execution, so
				// that signals, timers and other results reach it (another
				// task, here or elsewhere, runs them).
				d.handOver()
			}
		}
		if task == nil && d.held {
			// Nothing more to run here: give the lease back, keep the
			// executor for the next task.
			d.handOver()
		}
	}
	d.detach()
}

// detach hands the outcomes of activities still running to a recorder of
// their own (the driver's slot is freed meanwhile).
func (d *driver) detach() {
	if d.inflight == 0 {
		return
	}
	n, results, w := d.inflight, d.results, d.w
	d.inflight = 0
	w.tasks.Go(func() {
		for range n {
			r := <-results
			if _, _, err := w.record(context.WithoutCancel(w.taskCtx()), &r.task, r.outcome, nil); err != nil {
				w.log.LogAttrs(w.taskCtx(), slog.LevelWarn, "flows: record activity outcome",
					slog.String("activity", r.task.Name), slog.Any("error", err))
			}
			r.untrack()
		}
	})
}

// handOver releases the kept workflow lease; the executor goes to the cache
// for whoever takes the next task.
func (d *driver) handOver() {
	if !d.held {
		return
	}
	keep := d.ctx.Err() == nil // still ours: the executor is current
	d.release(0)
	if d.ex != nil && keep {
		d.w.cache.put(d.ex)
		d.ex = nil
	}
}

// recordResult records an eager result, resuming the workflow lease when
// resume (and the lease is held). It returns the resumed task, if any.
func (d *driver) recordResult(r eagerResult, resume bool) *store.WorkflowTask {
	d.inflight--
	delete(d.running, r.task.ScheduleID)
	defer r.untrack()
	var lease *store.Lease
	if resume && d.held && d.ctx.Err() == nil {
		lease = &d.lease
	}
	next, resumeLost, err := d.w.record(context.WithoutCancel(d.ctx), &r.task, r.outcome, lease)
	if err != nil {
		d.w.log.LogAttrs(d.ctx, slog.LevelWarn, "flows: record activity outcome",
			slog.String("activity", r.task.Name), slog.Any("error", err))
	}
	if resumeLost {
		d.lost()
	}
	return next
}

// lost records that the workflow lease is gone (taken over or released by
// the store).
func (d *driver) lost() {
	d.held = false
	d.untrack()
	d.evict()
}

// step executes one task and completes it; it returns the next task when
// the store asks for a re-run (undelivered signals), else nil.
func (d *driver) step(pollCtx context.Context, task *store.WorkflowTask) *store.WorkflowTask {
	w := d.w
	if d.ex == nil {
		d.ex = w.cache.take(task.Execution.ID, task.LastSeq)
	}
	if d.ex != nil && d.ex.LastSeq() != task.LastSeq {
		d.evict()
	}
	if d.ex == nil && !d.build(task) {
		return nil
	}
	c, err := d.ex.Execute(task)
	if err != nil {
		d.fail(task, err)
		return nil
	}
	stopping := pollCtx.Err() != nil
	eager := d.eager(c, stopping)
	switch {
	case d.inflight+len(eager) > 0 && !stopping:
		c.KeepFor = w.opts.LeaseTTL
	case !stopping: // a stopping worker must not keep the next task to itself
		c.StickyFor = w.opts.StickyFor
	}
	sent := time.Now()
	leases, err := w.st.CompleteWorkflowTask(d.ctx, d.lease, c)
	if err != nil {
		for range eager {
			release(w.actSlots, 1)
		}
		return d.completeFailed(task, err)
	}
	d.completed(task, c, eager, leases, sent)
	return nil
}

// completeFailed handles a rejected completion; it returns the task to
// run instead when the store asks for a re-run with undelivered events.
func (d *driver) completeFailed(task *store.WorkflowTask, err error) *store.WorkflowTask {
	w := d.w
	d.evict() // its state includes what was not stored
	switch {
	case errors.Is(err, store.ErrPendingEvents):
		next, rerr := w.st.ResumeWorkflowTask(d.ctx, d.lease, w.opts.LeaseTTL)
		if rerr == nil {
			return &next
		}
		if errors.Is(rerr, store.ErrLeaseLost) {
			d.lost()
		} else {
			d.release(0)
		}
	case errors.Is(err, store.ErrLeaseLost):
		d.lost()
	case errors.Is(context.Cause(d.ctx), errStopping):
		d.release(0) // the stop deadline passed: another instance takes over at once
	default:
		w.log.LogAttrs(d.ctx, slog.LevelError, "flows: complete workflow task",
			slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
		d.release(w.opts.HandBack)
	}
	return nil
}

// completed follows up a stored completion: lease and executor
// bookkeeping, local cancels, and the launch of eager activities.
func (d *driver) completed(task *store.WorkflowTask, c *store.Completion, eager []int, leases []store.Lease, sent time.Time) {
	w := d.w
	if c.KeepFor <= 0 {
		d.held = false
		d.untrack()
	} else {
		w.hb.renewedAt(false, d.lease.ID, sent) // the completion extended it to sent+KeepFor at least
	}
	if c.Status != "" && (!c.Status.Active() || c.Status == store.StatusNeedsAttention) {
		d.evict()
	} else if !d.held && d.inflight == 0 && len(eager) == 0 {
		w.cache.put(d.ex)
		d.ex = nil
	}
	// Activities this task canceled stop at once when they run here (others
	// learn it from their heartbeat).
	for _, sched := range c.CanceledActivities {
		if id, ok := d.running[sched]; ok {
			w.hb.cancel(true, id, ErrActivityCanceled)
		}
	}
	for i, idx := range eager {
		a := c.Activities[idx]
		d.launch(store.ActivityTask{
			Lease: leases[i], ExecutionID: task.Execution.ID, InstanceID: task.Execution.InstanceID,
			ScheduleID: a.ScheduleID, Name: a.Name, Queue: a.Queue, Input: a.Input, Compensate: a.Compensate,
			Attempt: 1,
		}, sent)
	}
}

// eager marks the activities this worker runs at once (registered here,
// with a free activity slot) and returns their indexes.
func (d *driver) eager(c *store.Completion, stopping bool) []int {
	w := d.w
	if !w.opts.Eager || stopping {
		return nil
	}
	var idx []int
	for i := range c.Activities {
		a := &c.Activities[i]
		if _, ok := w.acts.Activity(a.Name, a.Compensate); !ok || !tryAcquire(w.actSlots) {
			continue
		}
		a.ClaimFor = w.opts.LeaseTTL
		idx = append(idx, i)
	}
	return idx
}

// launch runs an eagerly claimed activity (its slot is held); its outcome
// goes to the driver.
func (d *driver) launch(t store.ActivityTask, claimed time.Time) {
	w := d.w
	actx, untrack := w.hb.track(w.taskCtx(), true, t.Lease, claimed)
	d.inflight++
	d.running[t.ScheduleID] = t.Lease.ID
	results := d.results
	w.tasks.Go(func() {
		defer release(w.actSlots, 1)
		o := attemptOutcome{lost: true}
		func() {
			defer func() {
				if r := recover(); r != nil { // attempt recovers user panics; this guards bugs
					w.log.LogAttrs(actx, slog.LevelError, "flows: panic running activity", slog.Any("panic", r))
				}
			}()
			if a, ok := w.acts.Activity(t.Name, t.Compensate); ok {
				o = w.attempt(actx, a, &t)
			}
		}()
		results <- eagerResult{task: t, outcome: o, untrack: untrack}
	})
}

// build creates the executor of a task's execution by replaying its
// history; false when the task was handed back or parked.
func (d *driver) build(task *store.WorkflowTask) bool {
	w := d.w
	e := &task.Execution
	wf, ok := w.wfs.Workflow(e.Workflow, e.Version)
	if !ok {
		// Another instance (a newer or older deploy) may know it: hand the
		// execution back instead of failing it (GW-12).
		w.log.LogAttrs(d.ctx, slog.LevelWarn, "flows: unknown workflow, handing back",
			slog.String("workflow", e.Workflow), slog.Int("version", int(e.Version)))
		d.release(w.opts.HandBack)
		return false
	}
	ex := executor.New(wf, e.ID, executor.Options{
		StuckTimeout: w.opts.StuckTimeout, ErrorText: w.opts.Errors.Text, ErrorKinds: w.opts.Errors.Kinds,
	})
	hist, err := w.st.LoadHistory(d.ctx, e.ID, 0)
	if err == nil {
		err = ex.Replay(hist)
	}
	if err != nil {
		ex.Close()
		d.fail(task, err)
		return false
	}
	d.ex = ex
	return true
}

// fail handles an executor error: non-determinism parks the execution for
// an operator (GW-11: never fail it); a stuck or broken executor hands the
// execution back.
func (d *driver) fail(task *store.WorkflowTask, err error) {
	w := d.w
	d.evict()
	if errors.Is(err, executor.ErrNonDeterministic) {
		w.log.LogAttrs(d.ctx, slog.LevelError, "flows: non-deterministic workflow, parking",
			slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
		c := &store.Completion{Status: store.StatusNeedsAttention, Err: w.opts.Errors.Text(err)}
		if _, cerr := w.st.CompleteWorkflowTask(d.ctx, d.lease, c); cerr != nil && !errors.Is(cerr, store.ErrLeaseLost) {
			w.log.LogAttrs(d.ctx, slog.LevelError, "flows: park", slog.Any("error", cerr))
			d.release(w.opts.HandBack)
			return
		}
		d.held = false
		d.untrack()
		return
	}
	if errors.Is(err, coro.ErrStuck) {
		err = fmt.Errorf("workflow code did not yield (blocking call?): %w", err)
	}
	w.log.LogAttrs(d.ctx, slog.LevelError, "flows: workflow task failed, handing back",
		slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
	d.release(w.opts.HandBack)
}

func (d *driver) evict() {
	if d.ex != nil {
		d.ex.Close()
		d.ex = nil
	}
}

// release gives the workflow lease back (after delay), if still held.
func (d *driver) release(delay time.Duration) {
	if !d.held {
		return
	}
	d.held = false
	err := d.w.st.ReleaseWorkflowTask(context.WithoutCancel(d.ctx), d.lease, delay)
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		d.w.log.LogAttrs(d.ctx, slog.LevelWarn, "flows: release workflow task", slog.Any("error", err))
	}
	d.untrack()
}
