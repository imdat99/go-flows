package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/imdat99/go-flows/internal/compensation"
	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
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
	ctx      context.Context       // canceled when the workflow lease is lost or at the stop deadline
	detached context.Context       // context.WithoutCancel(ctx), made once (recordResult)
	resumed  [2]store.WorkflowTask // reused for the tasks resumed with an activity's result
	eagerBuf []int                 // reused by eager
	flip     bool
	lease    store.Lease
	held     bool     // the workflow lease is held (kept between tasks)
	tracked  tracking // the workflow lease in the heartbeat (untrack)
	ex       *executor.Executor
	inflight int
	results  chan eagerResult
	running  map[int32]int64 // schedule id -> activity lease id of eager activities in flight
	set      *eagerSet       // results and running, from Worker.getEager
	window   *time.Timer     // reused for every wait on eager results
	// compensating: the workflow ended and its compensation runs
	// (compensate.go); ledger is its state after ledgerSeq, kept between
	// the tasks of this driver.
	compensating bool
	ledger       *compensation.Ledger
	ledgerSeq    int64
	// The outcome of the current task, for its span: the status committed
	// (needs_attention for a park; "" if unchanged or not stored) and the
	// error that ended it early (handed back, completion refused).
	taskStatus store.Status
	taskErr    error
}

// errHandedBack reports a task given back to the store for another
// instance (unknown workflow version) or a later retry.
var errHandedBack = errors.New("worker: task handed back")

// untrack stops tracking the workflow lease in the heartbeat.
func (d *driver) untrack() { d.tracked.untrack() }

type eagerResult struct {
	task    store.ActivityTask
	outcome attemptOutcome
	tracked tracking
}

// runWorkflowTask runs a claimed workflow task and everything it leads to.
func (w *Worker) runWorkflowTask(pollCtx context.Context, task *store.WorkflowTask, claimed time.Time) {
	tctx, tracked := w.hb.track(w.taskCtx(), false, task.Lease, claimed)
	// results and running are made by the first eager launch: most tasks
	// launch none (a channel of MaxActivities results is ~24 KiB).
	d := &driver{w: w, ctx: tctx, lease: task.Lease, held: true, tracked: tracked}
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
		d.window = d.w.getTimer(d.w.opts.EagerWindow)
	} else {
		d.window.Reset(d.w.opts.EagerWindow) // Go 1.23+ timers: no stale value to drain
	}
	return d.window.C
}

func (d *driver) run(pollCtx context.Context, task *store.WorkflowTask) {
	defer func() {
		if d.window != nil {
			d.w.putTimer(d.window)
			d.window = nil
		}
		if d.set != nil && d.inflight == 0 { // not handed to a detached recorder
			d.w.putEager(d.set)
			d.set, d.results, d.running = nil, nil, nil
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
	d.set, d.results, d.running = nil, nil, nil // the recorder owns the channel now
	w.detached.Add(1)                           // before the driver releases its slot
	w.tasks.Go(func() {
		defer w.detached.Add(-1)
		for range n {
			r := <-results
			if _, _, err := w.record(context.WithoutCancel(w.taskCtx()), &r.task, r.outcome, nil, nil); err != nil {
				w.log.LogAttrs(w.taskCtx(), slog.LevelError, "flows: activity outcome not recorded (it runs again after its lease expires)",
					slog.String("activity", r.task.Name), slog.Any("error", err))
			}
			r.tracked.untrack()
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
	defer r.tracked.untrack()
	var lease *store.Lease
	if resume && d.held && d.ctx.Err() == nil {
		lease = &d.lease
	}
	if d.detached == nil {
		d.detached = d.w.detachedCtx // d.ctx's values (none of its own), never canceled
	}
	into := d.nextResumeBuffer()
	next, resumeLost, err := d.w.record(d.detached, &r.task, r.outcome, lease, into)
	if err != nil {
		d.w.log.LogAttrs(d.ctx, slog.LevelError, "flows: activity outcome not recorded (it runs again after its lease expires)",
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
func (d *driver) step(pollCtx context.Context, task *store.WorkflowTask) (next *store.WorkflowTask) {
	w := d.w
	if w.opts.Tracer != nil {
		var end func(store.Status, error)
		w.observe(func() { end = w.opts.Tracer.StartTask(d.ctx, &task.Execution) })
		if end != nil {
			d.taskStatus, d.taskErr = "", nil
			defer func() { w.observe(func() { end(d.taskStatus, d.taskErr) }) }()
		}
	}
	c := d.execute(task)
	if c == nil {
		return nil
	}
	return d.complete(pollCtx, task, c)
}

// nextResumeBuffer returns the buffer for the next resumed task: one of
// two in turn, never the last one written (the task running now may be it,
// and still referenced while its follower is stored).
func (d *driver) nextResumeBuffer() *store.WorkflowTask {
	d.flip = !d.flip
	if d.flip {
		return &d.resumed[1]
	}
	return &d.resumed[0]
}

// complete stores a task's completion and follows it up.
func (d *driver) complete(pollCtx context.Context, task *store.WorkflowTask, c *store.Completion) *store.WorkflowTask {
	w := d.w
	stopping := pollCtx.Err() != nil
	eager := d.eager(c, stopping)
	for i := range c.Locks {
		// A lock outlives a lease of this worker (saga: raised to LeaseTTL).
		c.Locks[i].TTL = max(c.Locks[i].TTL, w.opts.LeaseTTL)
		c.Locks[i].Hold = w.opts.LockHold
	}
	c.Resume = nil
	switch {
	case (d.inflight+len(eager) > 0 || len(c.Locks) > 0) && !stopping:
		c.KeepFor = w.opts.LeaseTTL
		if len(c.Locks) > 0 {
			// The answers are pending as soon as it commits: the next task
			// comes back with the completion (T9.7, one round trip).
			c.Resume = d.nextResumeBuffer()
		}
	case !stopping && !d.compensating && w.cache.fits(d.ex): // only an executor kept in the cache gains from it
		c.StickyFor = w.opts.StickyFor
	}
	// Sticky, this worker wakes for the execution's next timer (whoever
	// committed it): the store leaves it to this worker (T10.3) when it
	// falls within DueKnown, half the horizon of the local due times (a
	// margin for the clocks of both).
	var nextTimer time.Time
	if c.StickyFor > 0 && d.ex != nil {
		var ok bool
		if nextTimer, ok = d.ex.NextTimer(); ok {
			c.DueKnown = w.wfDue.horizon / 2
		}
	}
	sent := time.Now()
	// The worker's task context, not d.ctx: a lease lost meanwhile is
	// caught by the store's fence anyway, and a long-lived context costs the
	// store's cancellation watch nothing per task (Stop's deadline still
	// cancels it).
	leases, err := w.st.CompleteWorkflowTask(w.taskCtx(), d.lease, c)
	if err != nil {
		c.Resume = nil // a resume buffer: not c's to keep
		for range eager {
			release(w.actSlots, 1)
		}
		if !errors.Is(err, store.ErrPendingEvents) { // a normal re-run, not a failure
			d.taskErr = err
		}
		return d.completeFailed(task, err)
	}
	d.taskStatus = c.Status
	d.reportDropped(task, c)
	for i := range c.Timers {
		// The timer's delay counts from the claim (task.Now), before sent:
		// the local due time is never before the store's.
		w.wfDue.add(sent.Add(c.Timers[i].At.Sub(task.Now)))
	}
	if !nextTimer.IsZero() {
		w.wfDue.add(sent.Add(nextTimer.Sub(task.Now)))
	}
	// Read before completed: it may hand c's executor to the cache.
	locks, next, resumed := len(c.Locks) > 0, c.Resume, c.Resumed
	c.Resume = nil
	d.completed(task, c, eager, leases, sent)
	if locks && d.held {
		// The answers are pending already: run the next task at once.
		if resumed {
			return next
		}
		d.lost() // kept, yet not resumed: the lease moved on meanwhile
	}
	return nil
}

// execute produces the completion of a task: by running the workflow, or,
// once it ended and compensates, by planning its compensation. nil when the
// task was handed back or parked.
func (d *driver) execute(task *store.WorkflowTask) *store.Completion {
	w := d.w
	if d.compensating || task.Execution.Status == store.StatusCompensating {
		d.compensating = true
		d.evict()
		if ex := w.cache.take(task.Execution.ID, -1); ex != nil { // a stale executor: never used again
			ex.Close()
		}
		c, err := d.compensate(task)
		if err != nil {
			d.taskErr = err
			w.log.LogAttrs(d.ctx, slog.LevelError, "flows: compensation task failed, handing back",
				slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
			d.release(w.opts.HandBack)
			return nil
		}
		return c
	}
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
	if (c.Status == store.StatusFailed || c.Status == store.StatusCanceled) && d.ex.MayCompensate() {
		if err := d.beginCompensation(task, c); err != nil {
			d.fail(task, fmt.Errorf("start compensation: %w", err))
			return nil
		}
		if c.Status != store.StatusFailed && c.Status != store.StatusCanceled {
			d.compensating = true // from now on, no workflow code runs
			d.evict()
		}
	}
	return c
}

// completeFailed handles a rejected completion; it returns the task to
// run instead when the store asks for a re-run with undelivered events.
func (d *driver) completeFailed(task *store.WorkflowTask, err error) *store.WorkflowTask {
	w := d.w
	d.evict() // its state includes what was not stored
	d.ledger = nil
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
	if store.Ends(c.Status) {
		w.ended(&task.Execution, c.Status, c.Err, c.ErrKinds)
	}
	if c.Status != "" && (!c.Status.Active() || c.Status == store.StatusNeedsAttention) {
		d.evict()
	} else if d.ex != nil && !d.held && d.inflight == 0 && len(eager) == 0 {
		// Last: c is the executor's own completion, which the next task of
		// the execution (another goroutine, once it is in the cache)
		// rewrites. Nothing below reads c when this branch runs (no eager
		// activity, no activity in flight to cancel).
		defer func() {
			w.cache.put(d.ex)
			d.ex = nil
		}()
	}
	// Activities this task canceled stop at once when they run here (others
	// learn it from their heartbeat).
	for _, sched := range c.CanceledActivities {
		if id, ok := d.running[sched]; ok {
			w.hb.cancel(true, id, ErrActivityCanceled)
		}
	}
	if len(leases) < len(eager) { // a store bug: never index past them
		w.log.LogAttrs(d.ctx, slog.LevelError, "flows: fewer eager leases than eager activities",
			slog.Int("leases", len(leases)), slog.Int("eager", len(eager)))
		release(w.actSlots, len(eager)-len(leases)) // the activities left are claimed by polls
		eager = eager[:len(leases)]
	}
	for i, idx := range eager {
		a := c.Activities[idx]
		d.launch(store.ActivityTask{
			Lease: leases[i], ExecutionID: task.Execution.ID, InstanceID: task.Execution.InstanceID,
			ScheduleID: a.ScheduleID, Name: a.Name, Queue: a.Queue, Input: a.Input, Compensate: a.Compensate,
			Uncertain: a.Uncertain, Attempt: 1,
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
	idx := d.eagerBuf[:0] // used within the task's completion only
	for i := range c.Activities {
		a := &c.Activities[i]
		// Only activities of a queue this worker runs (WithQueue routes
		// them to other engines), with a free slot.
		if _, ok := w.acts.Activity(a.Name, a.Compensate); !ok || !w.runsQueue(a.Queue) || !tryAcquire(w.actSlots) {
			continue
		}
		a.ClaimFor = w.opts.LeaseTTL
		idx = append(idx, i)
	}
	d.eagerBuf = idx
	return idx
}

// runsQueue reports whether the worker runs tasks of queue q.
func (w *Worker) runsQueue(q string) bool { return slices.Contains(w.opts.Queues, q) }

// launch runs an eagerly claimed activity (its slot is held); its outcome
// goes to the driver.
func (d *driver) launch(t store.ActivityTask, claimed time.Time) {
	w := d.w
	actx, tracked := w.hb.track(w.taskCtx(), true, t.Lease, claimed)
	if d.results == nil {
		// Buffered for every activity that can run at once: an activity
		// never blocks on a driver that stopped reading (panic, hand-over).
		d.set = w.getEager()
		d.results, d.running = d.set.results, d.set.running
	}
	d.inflight++
	d.running[t.ScheduleID] = t.Lease.ID
	w.runEager(eagerJob{ctx: actx, task: t, tracked: tracked, results: d.results})
}

// eagerJob is an eagerly claimed activity for a runner (runEager).
type eagerJob struct {
	ctx     context.Context //nolint:containedctx // the attempt's context, handed to the runner
	task    store.ActivityTask
	tracked tracking
	results chan<- eagerResult
}

// runEager runs job on an idle runner goroutine, or on a new one: runners
// are reused (a goroutine and its closure per activity cost allocations).
// Each job holds an activity slot, so runners stay within about twice
// MaxActivities (a runner that just released its slot may not be idle
// yet). Jobs count in tasks (Stop waits for them).
func (w *Worker) runEager(job eagerJob) {
	w.tasks.Add(1)
	select {
	case w.eagerJobs <- job:
		return
	default:
	}
	w.spawnRunner(job)
}

// spawnRunner starts a runner with its first job (a function of its own:
// the job captured by the closure escapes only here, not on every
// runEager; inlined, it would move every job to the heap).
//
//go:noinline
func (w *Worker) spawnRunner(job eagerJob) {
	w.runners.Go(func() {
		for {
			w.eager1(&job)
			select {
			case job = <-w.eagerJobs:
			case <-w.runnersStop:
				return
			}
		}
	})
}

// eager1 runs one eager activity and hands its outcome to its driver.
func (w *Worker) eager1(job *eagerJob) {
	defer w.tasks.Done()
	defer release(w.actSlots, 1)
	o := attemptOutcome{lost: true}
	func() {
		defer func() {
			if r := recover(); r != nil { // attempt recovers user panics; this guards bugs
				w.log.LogAttrs(job.ctx, slog.LevelError, "flows: panic running activity", slog.Any("panic", r))
			}
		}()
		if a, ok := w.acts.Activity(job.task.Name, job.task.Compensate); ok {
			o = w.attempt(job.ctx, a, &job.task)
		}
	}()
	job.results <- eagerResult{task: job.task, outcome: o, tracked: job.tracked}
	*job = eagerJob{} // the runner keeps no reference while idle
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
		d.taskErr = fmt.Errorf("%w: unknown workflow %s@%d", errHandedBack, e.Workflow, e.Version)
		d.release(w.opts.HandBack)
		return false
	}
	ex := executor.New(wf, e.ID, w.exOpts)
	var err error
	if task.LastSeq > 0 { // a new execution has no history yet
		var hist []history.Event
		if hist, err = w.st.LoadHistory(d.ctx, e.ID, 0); err == nil {
			err = ex.Replay(hist)
		}
	}
	if err != nil {
		ex.Close()
		d.fail(task, err)
		return false
	}
	d.ex = ex
	return true
}

// fail handles an executor error. Wrong workflow code parks the execution
// for an operator (GW-11: never fail it): non-determinism, or code that did
// not yield twice at the same point of its history (once may be a stall of
// the process: it is handed back; every further attempt would leak a
// spinning goroutine). Any other error (an unknown event type, a broken
// executor) hands the execution back.
func (d *driver) fail(task *store.WorkflowTask, err error) {
	w := d.w
	d.evict()
	park := errors.Is(err, executor.ErrNonDeterministic)
	if errors.Is(err, coro.ErrStuck) {
		err = fmt.Errorf("workflow code did not yield (blocking call or busy loop?): %w", err)
		// Counted in the store: a second stall here, on any engine, parks
		// (T9.3). Unrecorded (an error), it is handed back.
		again, nerr := w.st.NoteStuck(d.ctx, d.lease, task.LastSeq)
		if nerr != nil && !errors.Is(nerr, store.ErrLeaseLost) {
			w.log.LogAttrs(d.ctx, slog.LevelWarn, "flows: could not count a stall, handing back",
				slog.String("instance", task.Execution.InstanceID), slog.Any("error", nerr))
		}
		park = nerr == nil && again
	}
	d.taskErr = err
	if park {
		w.log.LogAttrs(d.ctx, slog.LevelError, "flows: workflow code is wrong, parking",
			slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
		c := &store.Completion{Status: store.StatusNeedsAttention, Err: w.opts.Errors.Text(err), ErrKinds: w.opts.Errors.Kinds(err)}
		_, cerr := w.st.CompleteWorkflowTask(d.ctx, d.lease, c)
		if cerr != nil && !errors.Is(cerr, store.ErrLeaseLost) {
			w.log.LogAttrs(d.ctx, slog.LevelError, "flows: park", slog.Any("error", cerr))
			d.release(w.opts.HandBack)
			return
		}
		d.held = false
		d.untrack()
		if cerr == nil {
			d.taskStatus = c.Status
			w.ended(&task.Execution, c.Status, c.Err, c.ErrKinds)
		}
		return
	}
	w.log.LogAttrs(d.ctx, slog.LevelError, "flows: workflow task failed, handing back",
		slog.String("instance", task.Execution.InstanceID), slog.Any("error", err))
	d.release(w.opts.HandBack)
}

// ended tells the observer of a committed end or park.
func (w *Worker) ended(e *store.Execution, status store.Status, msg, kinds string) {
	if w.opts.Observer != nil {
		w.observe(func() {
			w.opts.Observer.Ended(store.EndInfo{
				ExecutionID: e.ID, InstanceID: e.InstanceID, Workflow: e.Workflow, Version: e.Version,
				Status: status, Err: msg, ErrKinds: kinds,
			})
		})
	}
}

// observe calls the observer, recovering (and logging) its panics: a
// broken metrics hook must not lose a result nor crash the process.
func (w *Worker) observe(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			w.log.LogAttrs(context.Background(), slog.LevelError, "flows: panic in observer", slog.Any("panic", r))
		}
	}()
	fn()
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

// reportDropped logs (and tells the dropped hook) what an ending workflow
// started in its last task without awaiting it, which was not started
// (T9.2: not silent).
func (d *driver) reportDropped(task *store.WorkflowTask, c *store.Completion) {
	if c.Status == "" || c.Status.Active() || d.ex == nil {
		return
	}
	acts, kids := d.ex.DroppedAtEnd()
	if acts+kids == 0 {
		return
	}
	w := d.w
	w.log.LogAttrs(d.ctx, slog.LevelWarn, "flows: workflow ended with activities or children it did not await: not started",
		slog.String("instance", task.Execution.InstanceID), slog.String("status", string(c.Status)),
		slog.Int("activities", acts), slog.Int("children", kids))
	if w.droppedHook != nil {
		w.callDroppedHook(task.Execution.InstanceID, task.Execution.ID, acts, kids)
	}
}

// callDroppedHook calls the dropped hook, recovering (and logging) its
// panics: the task is committed, the rest of its completion must run.
func (w *Worker) callDroppedHook(instanceID string, executionID int64, acts, kids int) {
	defer func() {
		if r := recover(); r != nil {
			w.log.LogAttrs(context.Background(), slog.LevelError, "flows: panic in dropped hook",
				slog.String("instance", instanceID), slog.Any("panic", r))
		}
	}()
	w.droppedHook(instanceID, executionID, acts, kids)
}
