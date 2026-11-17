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
	"bytes"
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
	// ZeroComp is the encoding of the zero compensation data (nil if
	// unknown): an attempt that returns it returned no data, so the data of
	// an earlier attempt that may have had its effect is kept.
	ZeroComp []byte
}

// noComp reports whether comp is no compensation data (empty or zero).
func (a *Activity) noComp(comp []byte) bool {
	return len(comp) == 0 || a.ZeroComp != nil && bytes.Equal(comp, a.ZeroComp)
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

// DefaultCacheBytes is the default bound on the estimated bytes of the
// cached executors: 256 MiB, room for the default 1000 executors at about
// 256 KiB each.
const DefaultCacheBytes = 256 << 20

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
	// PollMin and PollMax bound the idle polling backoff. PollMax is at
	// most LeaseTTL/3 (recovery of a crashed engine's leases, T10.1).
	PollMin, PollMax time.Duration
	// HandBack delays the next claim of an execution this worker cannot
	// run (unknown workflow version, executor stuck).
	HandBack time.Duration
	// CacheSize is the number of executors kept between tasks.
	CacheSize int
	// CacheBytes bounds the estimated bytes of the cached executors
	// (executor.Size; default DefaultCacheBytes, -1: bounded by count
	// only). Beyond it the least recently used are evicted.
	CacheBytes int64
	// Eager runs a workflow task's activities in this worker at once.
	Eager bool
	// EagerWindow is how long the workflow lease stays held, waiting for
	// eager activities to finish, before it is released: until then,
	// signals, timers and other results wait for the next activity result.
	EagerWindow time.Duration
	// StuckTimeout bounds workflow code running without yielding.
	StuckTimeout time.Duration
	// LockHold is store.LockRequest.Hold for the locks this worker takes
	// (0: default 1h; negative: none).
	LockHold time.Duration
	Errors   ErrorCodec
	Logger   *slog.Logger
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
	// completion refused, a park's reason). The returned context is the
	// task's (StartChild).
	StartTask(ctx context.Context, e *store.Execution) (context.Context, func(status store.Status, err error))
	// StartFrom starts ne, a child of from or (continued) its
	// continue-as-new, under ctx (the context of the task starting it) and
	// sets ne.Trace; end follows its storing (created: false when the
	// child's id was in use).
	StartFrom(ctx context.Context, from *store.Execution, ne *store.NewExecution, continued bool) func(created bool, err error)
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
	switch {
	case o.LockHold == 0:
		o.LockHold = time.Hour
	case o.LockHold < 0:
		o.LockHold = 0
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
	// Recovery (T10.1): nothing announces the leases of a crashed engine
	// running out, and while its leases were renewed no claim could report
	// them due. Idle pollers poll at least as often as leases are renewed,
	// so the work of a crashed engine with the same LeaseTTL resumes within
	// about 4/3 of it, whatever PollMax.
	o.PollMax = min(o.PollMax, max(o.LeaseTTL/3, time.Millisecond))
	o.PollMin = min(o.PollMin, o.PollMax)
	if o.HandBack <= 0 {
		o.HandBack = 5 * time.Second
	}
	if o.EagerWindow <= 0 {
		o.EagerWindow = 200 * time.Millisecond
	}
	if o.CacheSize == 0 {
		o.CacheSize = 1000
	}
	if o.CacheBytes == 0 {
		o.CacheBytes = DefaultCacheBytes
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
	// detachedCtx is context.WithoutCancel(tasksCtx), made once; writes
	// detached from their task get a bounded context derived from it
	// (writeCtx).
	detachedCtx context.Context //nolint:containedctx // the worker's lifetime
	writes      atomic.Pointer[boundedCtx]
	// wfJobs hands claimed workflow tasks to idle runners (runTask).
	wfJobs chan wfJob
	// droppedHook, if set (SetDroppedHook), is told what an ending workflow
	// did not start (flowtest).
	droppedHook func(instanceID string, executionID int64, activities, children int)
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
	// keyEpoch is the store's key epoch (store.KeyEpocher), read at Start:
	// activities of executions below it keep the legacy key format.
	keyEpoch int64

	mu sync.Mutex
	// pollCancel stops claiming (Stop's first step); taskCancel ends the
	// tasks still running at Stop's deadline. Tasks run under tasksCtx, so
	// a stop lets them finish (and the heartbeat keep their leases) first.
	pollCancel context.CancelFunc
	tasksCtx   context.Context //nolint:containedctx // the lifetime of the worker's tasks
	taskCancel context.CancelCauseFunc
	// claimsCtx is the context of the claims (claimCtx); Stop cancels it
	// once the claims in flight had stopGrace to complete. stopping, set
	// as Stop begins, keeps new claims from starting.
	claimsCtx    context.Context //nolint:containedctx // the lifetime of the worker's claims
	claimsCancel context.CancelFunc
	stopping     atomic.Bool
	pollers      sync.WaitGroup // pollers and watch
	beat         sync.WaitGroup // heartbeat
	tasks        sync.WaitGroup // in-flight tasks
	// detached counts the recorders of detached eager outcomes (driver
	// detach): the only tasks that hold no slot (Busy).
	detached atomic.Int64
	// eagerStats counts eager launches and misses (T10.21).
	eagerStats eagerStats
	started    bool
}

// eagerStats counts, per worker, the activities run eagerly and those that
// could have been but found no free activity slot (they go through the
// queue: a NOTIFY and a claim more); missedInClaim counts the misses while
// an activity claim held slots (it takes every free slot but a spare for
// its round trip). A diagnostic, read only by tests and benchmarks
// (export_test.go): atomic adds, no allocation, negligible cost.
type eagerStats struct {
	launched, missed, missedInClaim atomic.Int64
	// claiming is set while an activity claim is in flight (its poller
	// holds the slots it took).
	claiming atomic.Bool
}

// SetDroppedHook sets a function told, when an execution of a workflow
// ends, of the activities and children it started in its last task without
// awaiting them, which were not started (they are also logged). Its panics
// are recovered and logged. Call before Start.
func (w *Worker) SetDroppedHook(f func(instanceID string, executionID int64, activities, children int)) {
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
		cache:       newCache(opts.CacheSize, opts.CacheBytes),
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
	if ke, ok := w.st.(store.KeyEpocher); ok {
		epoch, err := ke.KeyEpoch(ctx)
		if err != nil {
			return fmt.Errorf("worker: start: %w", err)
		}
		w.keyEpoch = epoch
	}
	w.started = true
	base := context.WithoutCancel(ctx)
	pctx, pollCancel := context.WithCancel(base)
	w.tasksCtx, w.taskCancel = context.WithCancelCause(base)
	w.claimsCtx, w.claimsCancel = context.WithCancel(base)
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

// boundedCtx is a context of detached writes (writeCtx): it ends at its
// deadline only; the next one is made from renew on.
type boundedCtx struct {
	ctx    context.Context //nolint:containedctx // shared by the writes of LeaseTTL/4
	cancel context.CancelFunc
	renew  time.Time
}

// writeCtx returns the context of a write detached from its task's (the
// outcome of an activity, the release of a lease): Stop's deadline does not
// cancel it, so that what is done is recorded, but it ends between
// 3/4 LeaseTTL and LeaseTTL from now (T11.2): a database that stopped
// answering (no RST) would otherwise hold it, and its task, forever. An
// outcome given up this way is not lost but redelivered (at least once):
// the activity's lease expires and the activity runs again.
//
// The context is shared by the writes of a quarter of LeaseTTL, renewed by the
// first write after that: no allocation per write, and the store watches
// it once (pg's fast path watches a context, not each call).
func (w *Worker) writeCtx() context.Context {
	now := time.Now()
	b := w.writes.Load()
	if b != nil && now.Before(b.renew) {
		return b.ctx
	}
	ctx, cancel := context.WithDeadline(w.detachedCtx, now.Add(w.opts.LeaseTTL))
	nb := &boundedCtx{ctx: ctx, cancel: cancel, renew: now.Add(w.opts.LeaseTTL / 4)}
	if !w.writes.CompareAndSwap(b, nb) {
		cancel() // another write renewed it first
		return w.writes.Load().ctx
	}
	// The one replaced ends at its deadline: writes may still use it.
	return ctx
}

// claimCtx is the context of the claims, not the polls' (Stop cancels
// those first). A claim in flight when Stop begins may commit in the
// store anyway; canceled, its tasks were lost to this worker yet stayed
// leased to it for a whole LeaseTTL, and other workers found them only at
// a timed poll (kept leases are not due times). So Stop lets it complete
// (its tasks run as the stop's) for stopGrace, or until Stop's deadline
// if sooner, then cancels it: a claim that does not return (a database
// out of reach) does not hold up the stop of an idle worker.
func (w *Worker) claimCtx() context.Context { return w.claimsCtx }

// stopGrace bounds Stop's wait for the claims in flight.
const stopGrace = time.Second

// errStopping is the cause of the task contexts canceled at Stop's
// deadline: an activity interrupted this way did not fail, its outcome is
// not recorded, and another instance runs it again once its lease expires.
var errStopping = errors.New("worker stopping")

// Stop stops claiming (no new claim is started after it begins, apart from
// one already being issued; a claim in flight completes within stopGrace
// and ctx, else it is canceled), then waits for in-flight tasks until ctx
// is done; tasks still running then are canceled, and only then does Stop
// return ctx's error (an idle worker whose deadline passes returns nil). Workflow leases kept for eager
// execution are released as their tasks end, so another instance can
// continue at once.
//
// Once ctx is done, Stop waits stopGrace at most for the canceled tasks
// and the rest of the shutdown (T11.2): a task blocked on a database that
// stopped answering (a failover, a partition without RST), or an activity
// that ignores its context's cancellation, is abandoned (before T11.2 Stop
// waited for it indefinitely). Its leases expire, so another instance takes
// its work over. A task blocked on the store ends by itself within about
// LeaseTTL (writes detached from their task are bounded, writeCtx; the
// others were canceled); until then it holds its pool connection, and
// closing the pool waits for it.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	pollCancel, taskCancel := w.pollCancel, w.taskCancel
	w.mu.Unlock()
	if pollCancel == nil {
		return nil
	}
	w.stopping.Store(true) // no claim starts from now on
	pollCancel()
	// over ends stopGrace after ctx: what Stop still waits for once ctx is
	// done.
	over, overCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer overCancel()
	var (
		graceMu  sync.Mutex
		grace    *time.Timer
		returned bool
	)
	stopOver := context.AfterFunc(ctx, func() {
		graceMu.Lock()
		defer graceMu.Unlock()
		if !returned {
			grace = time.AfterFunc(stopGrace, overCancel)
		}
	})
	defer func() {
		stopOver()
		graceMu.Lock()
		returned = true
		if grace != nil {
			grace.Stop()
		}
		graceMu.Unlock()
	}()
	// A claim in flight completes (claimCtx), within stopGrace and the
	// deadline; canceled after, it returns promptly.
	gctx, gcancel := context.WithTimeout(ctx, stopGrace)
	if !waitUntilDone(gctx, w.pollers.Wait) {
		w.claimsCancel()
		waitUntilDone(over, w.pollers.Wait)
	}
	gcancel()
	w.claimsCancel()
	var err error
	tasksDone := make(chan struct{})
	go func() {
		w.pollers.Wait() // abandoned above, a poller may still start tasks
		w.tasks.Wait()
		close(tasksDone)
	}()
	select {
	case <-tasksDone:
	case <-ctx.Done():
		if w.Busy() { // not an idle worker whose deadline came first
			err = fmt.Errorf("worker: stop: %w", ctx.Err())
		}
	}
	taskCancel(errStopping) // canceled tasks return promptly, unless blocked on a store that does not answer
	drained := waitUntilDone(over, func() {
		<-tasksDone
		w.runnersOnce.Do(func() { close(w.runnersStop) }) // the tasks drained: no job is in flight
		w.runners.Wait()
		w.beat.Wait()
	})
	if !drained {
		w.log.LogAttrs(ctx, slog.LevelWarn, "flows: stop: tasks abandoned, still running after the stop deadline (blocked on the store, or ignoring cancellation); their leases expire",
			slog.Int("workflow_slots", len(w.wfSlots)), slog.Int("activity_slots", len(w.actSlots)))
	} else if b := w.writes.Load(); b != nil {
		b.cancel() // no write is left: free its timer
	}
	// The cached executions are sticky to this worker: hand them over now,
	// not when their stickiness expires.
	w.cache.close()
	w.unstick(ctx, over)
	return err
}

// unstickWait is how long Stop waits for Unstick once its own deadline
// (over) has passed.
const unstickWait = 250 * time.Millisecond

// unstick hands the executions sticky to this worker over (Unstick), with
// a budget of its own (min(5s, LeaseTTL)), detached from Stop's: tasks
// abandoned at the deadline do not make it fail, and sticky executions are
// not left waiting StickyFor. Stop waits for it until over is done, and
// unstickWait more; on a store that does not answer, it goes on alone,
// within its budget.
func (w *Worker) unstick(ctx, over context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(5*time.Second, w.opts.LeaseTTL))
		defer cancel()
		if err := w.st.Unstick(uctx, w.opts.Owner); err != nil {
			w.log.LogAttrs(uctx, slog.LevelWarn, "flows: unstick on stop", slog.Any("error", err))
		}
	}()
	t := time.NewTimer(unstickWait)
	defer t.Stop()
	select {
	case <-done:
	case <-over.Done():
		select {
		case <-done:
		case <-t.C:
		}
	}
}

// waitUntilDone runs wait and reports whether it returned before ctx was
// done (if not, wait still runs).
func waitUntilDone(ctx context.Context, wait func()) bool {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
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
		started := time.Now()
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
		if time.Since(started) > w.opts.PollMax {
			backoff = w.opts.PollMin // it ran: a fresh drop, not a failing restart
		}
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

// acquire takes the free slots without blocking but spare of them, and at
// least one, blocking until one is free; it returns 0 when ctx is done.
// The activity claims keep a spare slot (T10.21): they hold their slots for
// a round trip, and an eager launch meanwhile would find none, its activity
// going through the queue (a NOTIFY waking every poller, a claim more).
func acquire(ctx context.Context, slots chan struct{}, spare int) int {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < cap(slots) && len(slots) < cap(slots)-spare {
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

// eagerSpare is the number of activity slots a claim leaves free for eager
// launches: one when the worker runs activities eagerly. Eager is on by
// default, so a worker that only polls activities (no workflow of its own
// scheduling them) keeps the spare too. Nothing starves: a claim that
// fills every slot it took loops again at once, and a single free slot is
// taken. But refilling two or more freed slots takes two round trips (the
// second claim takes the spare), MaxActivities 2 always claims one row at a
// time, and MaxActivities 1 is unchanged.
func eagerSpare(eager bool) int {
	if eager {
		return 1
	}
	return 0
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

// poll is the claim loop shared by both task kinds; each claim leaves
// spare free slots when more are free (acquire).
func (w *Worker) poll(ctx context.Context, slots chan struct{}, spare int, hint chan struct{}, due *dueTimes, claim func(n int) (int, error)) {
	idle := w.opts.PollMin
	t := time.NewTimer(idle)
	defer t.Stop()
	timedPoll := false // this claim follows a backoff timeout
	for {
		n := acquire(ctx, slots, spare)
		if n == 0 {
			return
		}
		if w.stopping.Load() {
			// Stop began meanwhile (acquire may pick a free slot over
			// ctx.Done): only a claim already in flight may complete.
			release(slots, n)
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
	cctx := w.claimCtx()
	w.poll(ctx, w.wfSlots, 0, w.wfHint, w.wfDue, func(n int) (int, error) {
		at := time.Now()
		var tasks []store.WorkflowTask
		var next time.Duration
		var err error
		if dc != nil && wantDue {
			tasks, next, err = dc.ClaimWorkflowTasksDue(cctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		} else {
			tasks, err = w.st.ClaimWorkflowTasks(cctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
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
	cctx := w.claimCtx()
	w.poll(ctx, w.actSlots, eagerSpare(w.opts.Eager), w.actHint, w.actDue, func(n int) (int, error) {
		at := time.Now()
		var tasks []store.ActivityTask
		var next time.Duration
		var err error
		w.eagerStats.claiming.Store(true)
		if dc != nil && wantDue {
			tasks, next, err = dc.ClaimActivityTasksDue(cctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		} else {
			tasks, err = w.st.ClaimActivityTasks(cctx, w.opts.Owner, w.opts.Queues, n, w.opts.LeaseTTL)
		}
		w.eagerStats.claiming.Store(false)
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
