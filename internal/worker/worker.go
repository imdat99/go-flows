// Package worker runs workflow and activity tasks from a store: it polls,
// executes, keeps leases alive and shuts down cleanly.
//
// Ported in role from go-workflows internal/worker (MIT, see NOTICE),
// redesigned around the store contract:
//   - Leases: a batched heartbeat with busy/lost semantics (heartbeat.go).
//     A lost lease cancels the task's context, activities included (GW-6,
//     GW-13).
//   - Eager execution: a workflow task's activities run in the same worker
//     right away, and each result resumes the workflow in the same round
//     trip (driver.go).
//   - Engine-side retries (activity.go, GW-15).
//   - Pollers back off when idle and wake on store hints (GW-8).
//   - Every boundary recovers panics (GW-10).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
)

// Workflows finds the workflow to run for an execution.
type Workflows interface {
	Workflow(name string, version int32) (executor.Workflow, bool)
}

// Activities finds the activity (or compensator) to run for a task.
type Activities interface {
	Activity(name string, compensate bool) (*Activity, bool)
}

// Activity is a registered activity or compensator.
type Activity struct {
	// Run executes one attempt. It returns the output and the compensation
	// data, or an error.
	Run func(ctx context.Context, t *store.ActivityTask) (out, comp []byte, err error)
	// Retry decides on failed attempts; the zero policy never retries.
	Retry RetryPolicy
	// Timeout bounds one attempt (0: none; the lease still bounds a dead
	// worker).
	Timeout time.Duration
}

// ErrorCodec records errors: their storage-safe text, their sentinel kinds
// (comma separated), and whether they are permanent (never retried).
type ErrorCodec interface {
	Text(err error) string
	Kinds(err error) string
	Permanent(err error) bool
}

type plainErrors struct{}

func (plainErrors) Text(err error) string { return err.Error() }
func (plainErrors) Kinds(error) string    { return "" }
func (plainErrors) Permanent(error) bool  { return false }

// Options configure a worker.
type Options struct {
	// Owner identifies this worker in leases; unique per process.
	Owner string
	// Queues the worker takes tasks from.
	Queues []string
	// LeaseTTL is the lease duration; the heartbeat renews every third.
	LeaseTTL time.Duration
	// StickyFor keeps an execution's next task on this worker while its
	// executor is cached.
	StickyFor time.Duration
	// MaxWorkflowTasks and MaxActivities bound concurrency.
	MaxWorkflowTasks int
	MaxActivities    int
	// PollMin and PollMax bound the idle polling backoff.
	PollMin, PollMax time.Duration
	// HandBack delays the next claim of an execution this worker cannot
	// run (unknown workflow version, executor stuck).
	HandBack time.Duration
	// CacheSize is the number of executors kept between tasks.
	CacheSize int
	// Eager runs a workflow task's activities in this worker at once.
	Eager bool
	// EagerWindow is how long the workflow lease stays held, waiting for
	// eager activities to finish, before it is released: until then,
	// signals, timers and other results wait for the next activity result.
	EagerWindow time.Duration
	// StuckTimeout bounds workflow code running without yielding.
	StuckTimeout time.Duration
	Errors       ErrorCodec
	Logger       *slog.Logger
	// Observer, when set, is told of activity attempts and of executions
	// ending, after the fact (it must not block).
	Observer Observer
	// Tracer, when set, wraps activity attempts and workflow tasks (spans).
	Tracer Tracer
}

// Tracer wraps work in spans (OpenTelemetry: package otelflows). Its
// panics are recovered and logged.
type Tracer interface {
	// StartAttempt starts an activity or compensator attempt: the returned
	// context is the attempt's (spans the activity makes are children);
	// end gets the attempt's error.
	StartAttempt(ctx context.Context, t *store.ActivityTask) (context.Context, func(err error))
	// StartTask starts a workflow task of e; end gets the status the task
	// committed ("" when unchanged or nothing was stored; needs_attention
	// for a park) and the error that ended it early (handed back, the
	// completion refused, a park's reason).
	StartTask(ctx context.Context, e *store.Execution) func(status store.Status, err error)
}

// Observer receives measurements (see Options.Observer).
type Observer interface {
	// ActivityAttempt follows each attempt of an activity or compensator,
	// before its outcome is recorded (an attempt whose recording fails, its
	// lease lost, may run and be reported again): outcome is "completed",
	// "failed" or "retrying"; err is the attempt's error (nil when
	// completed). Panics are recovered and logged.
	ActivityAttempt(t *store.ActivityTask, outcome string, d time.Duration, err error)
	// Ended follows a committed end or park of an execution (store.Ends).
	Ended(e store.EndInfo)
}

func (o *Options) defaults() {
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	if o.StickyFor <= 0 {
		o.StickyFor = 10 * time.Second
	}
	if o.MaxWorkflowTasks <= 0 {
		o.MaxWorkflowTasks = 64
	}
	if o.MaxActivities <= 0 {
		o.MaxActivities = 64
	}
	if o.PollMin <= 0 {
		o.PollMin = 10 * time.Millisecond
	}
	if o.PollMax <= 0 {
		o.PollMax = time.Second
	}
	if o.HandBack <= 0 {
		o.HandBack = 5 * time.Second
	}
	if o.EagerWindow <= 0 {
		o.EagerWindow = 200 * time.Millisecond
	}
	if o.CacheSize == 0 {
		o.CacheSize = 1000
	}
	if o.Errors == nil {
		o.Errors = plainErrors{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Worker polls a store and runs tasks.
type Worker struct {
	st    store.Store
	wfs   Workflows
	acts  Activities
	opts  Options
	cache *cache
	hb    *heartbeat
	log   *slog.Logger

	wfSlots, actSlots chan struct{} // semaphores
	wfHint, actHint   chan struct{} // store hints, capacity 1
	// timers and eagerSets are reused by drivers: their eager window timer,
	// and the channel of eager results with the map of activities in
	// flight (a channel of MaxActivities results is ~24 KiB).
	// detachedCtx is context.WithoutCancel(tasksCtx), made once.
	detachedCtx context.Context //nolint:containedctx // the worker's lifetime
	// wfJobs hands claimed workflow tasks to idle runners (runTask).
	wfJobs chan wfJob
	// droppedHook, if set (SetDroppedHook), is told what an ending workflow
	// did not start (flowtest).
	droppedHook func(instanceID string, activities, children int)
	// exOpts are the options of every executor (built once: its method
	// values would be allocated per execution).
	exOpts    executor.Options
	timers    sync.Pool
	eagerSets sync.Pool
	// eagerJobs hands eager activities to idle runners (runEager);
	// runnersStop ends the runners once Stop drained the tasks.
	eagerJobs     chan eagerJob
	runnersStop   chan struct{}
	runnersOnce   sync.Once // Stop may run more than once
	runners       sync.WaitGroup
	wfDue, actDue *dueTimes // local wake-ups for committed delays

	mu sync.Mutex
	// pollCancel stops claiming (Stop's first step); taskCancel ends the
	// tasks still running at Stop's deadline. Tasks run under tasksCtx, so
	// a stop lets them finish (and the heartbeat keep their leases) first.
	pollCancel context.CancelFunc
	tasksCtx   context.Context //nolint:containedctx // the lifetime of the worker's tasks
	taskCancel context.CancelCauseFunc
	pollers    sync.WaitGroup // pollers and watch
	beat       sync.WaitGroup // heartbeat
	tasks      sync.WaitGroup // in-flight tasks
	// detached counts the recorders of detached eager outcomes (driver
	// detach): the only tasks that hold no slot (Busy).
	detached atomic.Int64
	started  bool
}

// SetDroppedHook sets a function told, when a workflow ends, of the
// activities and children it started in its last task without awaiting
// them, which were not started (they are also logged). Call before Start.
func (w *Worker) SetDroppedHook(f func(instanceID string, activities, children int)) {
	w.droppedHook = f
}

// Busy reports whether the worker is claiming or running anything (a test
// driver of virtual time waits for it to be idle before moving the clock).
// Every task holds a slot for its whole life (claims take them first),
// except detached recorders, counted apart.
func (w *Worker) Busy() bool {
	return len(w.wfSlots) > 0 || len(w.actSlots) > 0 || w.detached.Load() > 0
}

// New returns a stopped worker.
func New(st store.Store, wfs Workflows, acts Activities, opts Options) (*Worker, error) {
	opts.defaults()
	if opts.Owner == "" || len(opts.Queues) == 0 {
		return nil, errors.New("worker: Owner and Queues are required")
	}
	w := &Worker{
		st: st, wfs: wfs, acts: acts, opts: opts, log: opts.Logger,
		cache:       newCache(opts.CacheSize),
		hb:          newHeartbeat(st, opts.Owner, opts.LeaseTTL, opts.Logger),
		wfSlots:     make(chan struct{}, opts.MaxWorkflowTasks),
		actSlots:    make(chan struct{}, opts.MaxActivities),
		wfHint:      make(chan struct{}, 1),
		actHint:     make(chan struct{}, 1),
		eagerJobs:   make(chan eagerJob),
		wfJobs:      make(chan wfJob),
		runnersStop: make(chan struct{}),
		wfDue:       newDueTimes(dueGrain(opts), 64*opts.PollMax),
		actDue:      newDueTimes(dueGrain(opts), 64*opts.PollMax),
	}
	w.exOpts = executor.Options{StuckTimeout: opts.StuckTimeout, ErrorText: opts.Errors.Text, ErrorKinds: opts.Errors.Kinds}
	return w, nil
}

// dueGrain is the rounding of due times: 10 ms (at most 100 wake-ups a
// second), not below 1 ms however short PollMin is. With the horizon of 64
// timed polls, a set holds at most 64*PollMax/grain times (6400 by
// default).
func dueGrain(o Options) time.Duration {
	return max(min(o.PollMin, 10*time.Millisecond), time.Millisecond)
}

// Start starts polling. Tasks run under contexts derived from
// context.WithoutCancel(ctx); Stop ends them.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("worker: already started")
	}
	w.started = true
	base := context.WithoutCancel(ctx)
	pctx, pollCancel := context.WithCancel(base)
	w.tasksCtx, w.taskCancel = context.WithCancelCause(base)
	w.detachedCtx = context.WithoutCancel(w.tasksCtx)
	w.pollCancel = pollCancel
	w.beat.Go(func() { w.hb.run(w.tasksCtx) })
	w.pollers.Go(func() { w.pollWorkflows(pctx) })
	w.pollers.Go(func() { w.pollActivities(pctx) })
	if wt, ok := w.st.(store.Watcher); ok {
		w.pollers.Go(func() { w.watch(pctx, wt) })
	}
	return nil
}

func (w *Worker) taskCtx() context.Context { return w.tasksCtx }

// errStopping is the cause of the task contexts canceled at Stop's
// deadline: an activity interrupted this way did not fail, its outcome is
// not recorded, and another instance runs it again once its lease expires.
var errStopping = errors.New("worker stopping")

// Stop stops claiming, then waits for in-flight tasks until ctx is done;
// tasks still running then are canceled. Workflow leases kept for eager
// execution are released as their tasks end, so another instance can
// continue at once.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	pollCancel, taskCancel := w.pollCancel, w.taskCancel
	w.mu.Unlock()
	if pollCancel == nil {
		return nil
	}
	pollCancel()
	w.pollers.Wait()
	done := make(chan struct{})
	go func() {
		w.tasks.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("worker: stop: %w", ctx.Err())
		taskCancel(errStopping)
		<-done // canceled tasks return promptly
	}
	taskCancel(errStopping)
	w.runnersOnce.Do(func() { close(w.runnersStop) }) // the tasks drained: no job is in flight
	w.runners.Wait()
	w.beat.Wait()
	// The cached executions are sticky to this worker: hand them over now,
	// not when their stickiness expires.
	w.cache.close()
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	if uerr := w.st.Unstick(uctx, w.opts.Owner); uerr != nil {
		w.log.LogAttrs(uctx, slog.LevelWarn, "flows: unstick on stop", slog.Any("error", uerr))
	}
	cancel()
	return err
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// watch feeds store hints to the pollers, restarting with backoff.
func (w *Worker) watch(ctx context.Context, wt store.Watcher) {
	queues := make(map[string]bool, len(w.opts.Queues))
	for _, q := range w.opts.Queues {
		queues[q] = true
	}
	backoff := w.opts.PollMin
	for ctx.Err() == nil {
		err := wt.Watch(ctx, func(k store.WorkKind, q string) {
			if q != "" && !queues[q] { // "": every queue
				return
			}
			if k == store.ActivityWork {
				notify(w.actHint)
			} else {
				notify(w.wfHint)
			}
		})
		if ctx.Err() != nil {
			return
		}
		w.log.LogAttrs(ctx, slog.LevelWarn, "flows: watch failed", slog.Any("error", err))
		if !sleep(ctx, backoff) {
			return
		}
		backoff = min(2*backoff, w.opts.PollMax)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// acquire takes up to n free slots without blocking, and at least one,
// blocking until one is free; it returns 0 when ctx is done.
func acquire(ctx context.Context, slots chan struct{}) int {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < cap(slots) {
		select {
		case slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func release(slots chan struct{}, n int) {
	for range n {
		<-slots
	}
}

// tryAcquire takes one slot if free.
func tryAcquire(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// poll is the claim loop shared by both task kinds.
func (w *Worker) poll(ctx context.Context, slots, hint chan struct{}, due *dueTimes, claim func(n int) (int, error)) {
	idle := w.opts.PollMin
	t := time.NewTimer(idle)
	defer t.Stop()
	timedPoll := false // this claim follows a backoff timeout
	for {
		n := acquire(ctx, slots)
		if n == 0 {
			return
		}
		polled := time.Now()
		started, err := claim(n)
		release(slots, n-started)
		if err != nil && ctx.Err() == nil {
			w.log.LogAttrs(ctx, slog.LevelWarn, "flows: claim failed", slog.Any("error", err))
		}
		if started == n {
			// Every free slot was filled: more work is probably waiting.
			idle = w.opts.PollMin
			timedPoll = false
			continue
		}
		if started > 0 && timedPoll {
			// Work without hints (a watcher down, delays of other
			// workers): keep polling at a short interval while it comes.
			idle = w.opts.PollMin
		}
		// Ready work comes with a hint (store notifications), and work
		// this worker committed with a delay (timers, retry delays) with a
		// due time. The timed polls only find the rest: work other
		// workers delayed, expired leases. A hint therefore polls once
		// without resetting the backoff: resetting it made every hint cost
		// a series of empty polls.
		if err != nil {
			// A failed claim covered nothing: its due times stay, and it
			// backs off (no immediate retry while the store is down).
			polled = time.Time{}
		}
		timedPoll = false
		backoff := time.Now().Add(idle)
	sleep:
		for {
			wait, timed := time.Until(backoff), true
			if at, ok := due.next(polled); ok && err == nil {
				if d := time.Until(at); d < wait {
					wait, timed = max(d, 0), false
				}
			}
			t.Reset(wait) // Go 1.23+ timers: Reset discards a pending value
			select {
			case <-ctx.Done():
				return
			case <-hint:
				break sleep
			case <-due.changed:
				// A new earliest due time: sleep again until it (no claim
				// before it is due).
			case <-t.C:
				if timed {
					idle = min(2*idle, w.opts.PollMax)
					timedPoll = true
				}
				break sleep
			}
		}
	}
}

func (w *Worker) pollWorkflows(ctx context.Context) {
	dc, _ := w.st.(store.DueClaimer)
	// A full claim ignores due times (storeDue), so the claim after one
	// does not ask: if that one comes back partial, the due times are
	// learned at the next claim, PollMin later at most.
	wantDue := true
	w.poll(ctx, w.wfSlots, w.wfHint, w.wfDue, func(n int) (int, error) {
		at := time.Now()
		var tasks []store.WorkflowTask
		var next time.Duration
		var err error
		if dc != nil && wantDue {
			tasks, next, err = dc.ClaimWorkflowTasksDue(ctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		} else {
			tasks, err = w.st.ClaimWorkflowTasks(ctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		}
		for i := range tasks {
			w.runTask(wfJob{pollCtx: ctx, task: tasks[i], at: at})
		}
		storeDue(w.wfDue, next, len(tasks), n, err)
		wantDue = len(tasks) < n
		return len(tasks), err
	})
}

func (w *Worker) pollActivities(ctx context.Context) {
	dc, _ := w.st.(store.DueClaimer)
	wantDue := true // as in pollWorkflows
	w.poll(ctx, w.actSlots, w.actHint, w.actDue, func(n int) (int, error) {
		at := time.Now()
		var tasks []store.ActivityTask
		var next time.Duration
		var err error
		if dc != nil && wantDue {
			tasks, next, err = dc.ClaimActivityTasksDue(ctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		} else {
			tasks, err = w.st.ClaimActivityTasks(ctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		}
		for i := range tasks {
			t := tasks[i]
			w.tasks.Go(func() {
				defer release(w.actSlots, 1)
				w.runPolledActivity(ctx, &t, at)
			})
		}
		storeDue(w.actDue, next, len(tasks), n, err)
		wantDue = len(tasks) < n
		return len(tasks), err
	})
}

// getTimer returns a stopped-or-new timer set to fire after d.
func (w *Worker) getTimer(d time.Duration) *time.Timer {
	if t, ok := w.timers.Get().(*time.Timer); ok {
		t.Reset(d) // Go 1.23+ timers: no stale value survives Stop/Reset
		return t
	}
	return time.NewTimer(d)
}

// putTimer stops t and keeps it for getTimer.
func (w *Worker) putTimer(t *time.Timer) {
	t.Stop()
	w.timers.Put(t)
}

// eagerSet is a driver's channel of eager results and map of the eager
// activities in flight, reused through Worker.eagerSets.
type eagerSet struct {
	results chan eagerResult
	running map[int32]int64
}

// getEager returns an empty channel of eager results (buffered for every
// activity that can run at once: a runner never blocks on a driver that
// stopped reading) and an empty map.
func (w *Worker) getEager() *eagerSet {
	if s, ok := w.eagerSets.Get().(*eagerSet); ok {
		return s
	}
	return &eagerSet{results: make(chan eagerResult, w.opts.MaxActivities), running: map[int32]int64{}}
}

// putEager keeps an empty set (its map cleared) for getEager.
func (w *Worker) putEager(s *eagerSet) {
	if len(s.results) > 0 {
		return // not empty: drop it
	}
	clear(s.running)
	w.eagerSets.Put(s)
}

// wfJob is a claimed workflow task for a runner (runTask).
type wfJob struct {
	pollCtx context.Context //nolint:containedctx // handed to the runner
	task    store.WorkflowTask
	at      time.Time
}

// runTask runs a claimed workflow task (its slot is held) on an idle
// runner goroutine, or on a new one, as runEager does for activities: a
// goroutine with its closure per task costs allocations. Runners stay
// within about twice MaxWorkflowTasks and live until Stop. Jobs count in
// tasks (Stop waits for them).
func (w *Worker) runTask(job wfJob) {
	w.tasks.Add(1)
	select {
	case w.wfJobs <- job:
		return
	default:
	}
	w.spawnTaskRunner(job)
}

// spawnTaskRunner starts a workflow task runner with its first job (not
// inlined: the job would move to the heap on every runTask).
//
//go:noinline
func (w *Worker) spawnTaskRunner(job wfJob) {
	w.runners.Go(func() {
		for {
			w.task1(&job)
			select {
			case job = <-w.wfJobs:
			case <-w.runnersStop:
				return
			}
		}
	})
}

func (w *Worker) task1(job *wfJob) {
	defer w.tasks.Done()
	defer release(w.wfSlots, 1)
	w.runWorkflowTask(job.pollCtx, &job.task, job.at)
	*job = wfJob{} // the runner keeps no reference while idle
}

// storeDue adds the time at which, per the store (store.DueClaimer), the
// next work of the queues becomes claimable, after a claim that left slots
// free: delayed work committed by other workers is then claimed when due,
// not up to PollMax late (T9.1).
func storeDue(due *dueTimes, next time.Duration, claimed, n int, err error) {
	if err == nil && next > 0 && claimed < n {
		due.add(time.Now().Add(next)) // measured after the claim: never early
	}
}
