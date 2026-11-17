package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// RetryPolicy decides whether a failed activity attempt is retried, and
// after how long. Retries are done by the engine on the activity task
// (store.RetryActivityTask), not by the workflow: the history records only
// the final outcome, and changing a policy is never non-determinism (GW-15).
type RetryPolicy struct {
	Attempts int           // total attempts, the first included; <= 1: no retry
	Initial  time.Duration // delay before the second attempt
	Factor   float64       // growth per attempt; <= 1 means constant
	Max      time.Duration // cap on the delay (0: none)
}

// Delay returns the delay before attempt next (2 for the first retry):
// Initial * Factor^(next-2), capped by Max.
func (p RetryPolicy) Delay(next int32) time.Duration {
	d := float64(p.Initial)
	if p.Factor > 1 && next > 2 {
		d *= math.Pow(p.Factor, float64(next-2))
	}
	if p.Max > 0 && d > float64(p.Max) {
		return p.Max
	}
	if d > math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(d)
}

// attemptOutcome is the result of one attempt.
type attemptOutcome struct {
	err   error         // the attempt's error, for the observer
	ev    history.Event // ActivityCompleted or ActivityFailed
	retry bool          // retry after delay instead of recording ev
	delay time.Duration
	maybe bool   // the retried attempt may have had its effect
	keep  []byte // with maybe: its compensation data, kept for later attempts
	lost  bool   // the lease was lost: record nothing
}

// startAttempt calls the tracer, recovering its panics.
func (w *Worker) startAttempt(ctx context.Context, t *store.ActivityTask) (actx context.Context, end func(error)) {
	defer func() {
		if r := recover(); r != nil {
			w.log.LogAttrs(context.Background(), slog.LevelError, "flows: panic in tracer", slog.Any("panic", r))
			actx, end = ctx, func(error) {}
		}
	}()
	actx, end = w.opts.Tracer.StartAttempt(ctx, t)
	if end == nil {
		end = func(error) {}
	}
	if actx == nil {
		actx = ctx
	}
	return actx, end
}

// ErrUncertain is wrapped by an activity's error to mark a failure after
// which the activity may still have had its effect (its compensator runs).
var ErrUncertain = errors.New("outcome uncertain")

// ErrAttemptsExhausted is the error of an activity whose attempts reached
// its retry policy's limit while the last ones were lost: their lease
// expired without a result, as when the activity crashes its process (out
// of memory, os.Exit, a fatal runtime error). It is permanent and
// uncertain (T11.3).
var ErrAttemptsExhausted = errors.New("activity attempts exhausted without a result (the process running it crashed or lost its lease)")

// ErrCrashLoop is the reason of an execution parked because its workflow
// task was claimed more than Options.MaxTaskClaims times in a row without
// completing: its engines crashed or lost the lease mid-task (T11.3).
var ErrCrashLoop = errors.New("workflow task never completes (the process running it crashed or lost its lease each time)")

// nullData is the compensation data recorded for an uncertain failure of an
// activity that has a compensator but returned no data: it marks the
// outcome compensable (the compensator gets the zero value). Whether an
// activity has a compensator is decided where it ran and recorded with its
// outcome, so that a binary without the compensator never skips it.
var nullData = []byte("null")

// attempt runs one attempt of t under ctx (canceled when its lease is
// lost) and decides what to record: an activity outcome, or for a
// compensation task, CompensationCompleted or CompensationFailed.
func (w *Worker) attempt(ctx context.Context, a *Activity, t *store.ActivityTask) attemptOutcome {
	t.LegacyKey = t.ExecutionID < w.keyEpoch
	var start time.Time
	if w.opts.Observer != nil {
		start = time.Now()
	}
	runCtx := ctx // the tracer's context runs the activity; ctx keeps the cancel causes
	var o attemptOutcome
	if w.opts.Tracer != nil {
		var end func(error)
		runCtx, end = w.startAttempt(ctx, t)
		defer func() {
			err := o.err
			if o.lost {
				err = ErrLeaseLost // another owner records the outcome
			}
			w.traced(func() { end(err) })
		}()
	}
	o = w.attemptOnce(ctx, runCtx, a, t)
	if w.opts.Observer != nil && !o.lost {
		outcome := "completed"
		switch {
		case o.retry:
			outcome = "retrying"
		case o.ev.Type == history.ActivityFailed:
			outcome = "failed"
		}
		w.observe(func() { w.opts.Observer.ActivityAttempt(t, outcome, time.Since(start), o.err) })
	}
	if o.lost || o.retry {
		return o
	}
	if t.Compensate {
		if o.ev.Type == history.ActivityCompleted {
			o.ev = history.Event{Type: history.CompensationCompleted}
		} else {
			o.ev = history.Event{Type: history.CompensationFailed, Err: o.ev.Err, ErrKinds: o.ev.ErrKinds, Permanent: o.ev.Permanent}
		}
		return o
	}
	o.ev.CompData = w.finalComp(a, t, &o.ev)
	return o
}

// finalComp is the compensation data to record with the final outcome ev
// of invocation t.
func (w *Worker) finalComp(a *Activity, t *store.ActivityTask, ev *history.Event) []byte {
	if ev.Type != history.ActivityCompleted && !ev.Uncertain {
		return ev.CompData // a certain failure is not compensated
	}
	if !a.noComp(ev.CompData) {
		return ev.CompData
	}
	kept := len(t.CompData) > 0
	if !kept && (!ev.Uncertain || len(ev.CompData) > 0) {
		return ev.CompData
	}
	// Whether it is compensated is decided here, where it ran: without a
	// compensator in this binary, no data (no compensation nobody runs).
	if _, ok := w.acts.Activity(t.Name, true); !ok {
		return ev.CompData
	}
	if kept {
		// No data of its own: an earlier attempt that may have had its
		// effect returned some (T10.7), the most recent is kept.
		return t.CompData
	}
	return nullData
}

// attemptOnce runs the activity under runCtx (derived from ctx, possibly by
// a tracer) and classifies its outcome from ctx's cancel cause.
func (w *Worker) attemptOnce(ctx, runCtx context.Context, a *Activity, t *store.ActivityTask) attemptOutcome {
	actx := runCtx
	if a.Timeout > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeout(runCtx, a.Timeout)
		defer cancel()
	}
	if t.CancelRequested {
		// Canceled by the workflow before this attempt: do not run it. An
		// earlier attempt (a retry is pending) may have had an effect; its
		// compensator gets the kept data of such an attempt (the zero value
		// if none). Before any attempt, certain.
		return attemptOutcome{err: ErrActivityCanceled, ev: history.Event{
			Type: history.ActivityFailed, Err: ErrActivityCanceled.Error(), ErrKinds: w.opts.Errors.Kinds(ErrActivityCanceled),
			Permanent: true, Uncertain: t.Uncertain && !t.Compensate,
		}}
	}
	if t.Exhausted {
		// Its attempts reached the limit and the last one was lost (its
		// process crashed, perhaps because of the activity): not run again,
		// it fails for good (T11.3). The lost attempt may have had its
		// effect: uncertain (the claim marked it), compensated with the
		// kept data of such an attempt (the zero value if none).
		err := fmt.Errorf("%w: %d attempts", ErrAttemptsExhausted, t.Attempt-1)
		return attemptOutcome{err: err, ev: history.Event{
			Type: history.ActivityFailed, Err: w.opts.Errors.Text(err), ErrKinds: w.opts.Errors.Kinds(err),
			Permanent: true, Uncertain: t.Uncertain && !t.Compensate,
		}}
	}
	out, comp, err, panicked := safeRun(actx, a, t)
	if errors.Is(context.Cause(ctx), ErrLeaseLost) {
		// Another owner runs it now (or the execution ended): its outcome,
		// not ours, is recorded (GW-6: a lost lease is not a failure).
		return attemptOutcome{lost: true}
	}
	if err != nil && errors.Is(context.Cause(ctx), errStopping) {
		// Interrupted by the stop deadline: not a failure. Hand the task
		// back at once so another instance runs it again (the next claim
		// counts as an attempt: a shutdown uses one).
		return attemptOutcome{retry: true, err: err, maybe: true, keep: w.keep(a, t, comp)}
	}
	if err == nil {
		return attemptOutcome{ev: history.Event{Type: history.ActivityCompleted, Payload: out, CompData: comp}}
	}
	if errors.Is(context.Cause(ctx), ErrActivityCanceled) {
		// The workflow no longer wants it: no retry. It may have had part
		// of its effect.
		return attemptOutcome{err: err, ev: history.Event{
			Type: history.ActivityFailed, Err: w.opts.Errors.Text(err), ErrKinds: w.opts.Errors.Kinds(err),
			Permanent: true, Uncertain: true, CompData: comp,
		}}
	}
	// Uncertain: the attempt may have had its effect though it reports a
	// failure (panic, timeout). Its compensator still runs.
	uncertain := panicked || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUncertain)
	permanent := panicked || w.opts.Errors.Permanent(err)
	if !permanent && t.Attempt < int32(a.Retry.Attempts) {
		o := attemptOutcome{retry: true, delay: a.Retry.Delay(t.Attempt + 1), err: err, maybe: uncertain}
		if uncertain {
			o.keep = w.keep(a, t, comp)
		}
		return o
	}
	// An earlier attempt that may have had its effect makes the failure
	// uncertain too (a timeout, then a decline: the charge may stand).
	uncertain = uncertain || t.Uncertain && !t.Compensate
	return attemptOutcome{err: err, ev: history.Event{
		Type: history.ActivityFailed, Err: w.opts.Errors.Text(err), ErrKinds: w.opts.Errors.Kinds(err),
		Permanent: permanent, Uncertain: uncertain, CompData: comp,
	}}
}

// keep returns the compensation data of a retried attempt that may have had
// its effect, to keep for later attempts: nil (keep the earlier data) when
// it returned none, and for a compensation task.
func (*Worker) keep(a *Activity, t *store.ActivityTask, comp []byte) []byte {
	if t.Compensate || a.noComp(comp) {
		return nil
	}
	return comp
}

// panicError is an activity panic, permanent and with its stack.
type panicError struct {
	value any
	stack []byte
}

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v\n%s", e.value, e.stack) }

// Unwrap makes errors.Is(err, ErrPanic) match.
func (e *panicError) Unwrap() error { return ErrPanic }

// ErrPanic is wrapped by the error of an activity attempt that panicked.
var ErrPanic = errors.New("panic")

func safeRun(ctx context.Context, a *Activity, t *store.ActivityTask) (out, comp []byte, err error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			out, comp, err, panicked = nil, nil, &panicError{value: r, stack: debug.Stack()}, true
		}
	}()
	out, comp, err = a.Run(ctx, t)
	return out, comp, err, false
}

// runPolledActivity runs a task claimed by the activity poller and records
// its outcome (no workflow resume: the workflow is not held here).
func (w *Worker) runPolledActivity(ctx context.Context, t *store.ActivityTask, claimed time.Time) {
	tctx, tracked := w.hb.track(w.taskCtx(), true, t.Lease, claimed)
	defer tracked.untrack()
	a, ok := w.acts.Activity(t.Name, t.Compensate)
	if !ok {
		// Another instance may have it: give the task back for a while.
		if err := w.st.RetryActivityTask(tctx, t.Lease, w.opts.HandBack, false, nil); err != nil && !errors.Is(err, store.ErrLeaseLost) {
			w.log.LogAttrs(ctx, slog.LevelWarn, "flows: hand back activity", slog.String("activity", t.Name), slog.Any("error", err))
		}
		return
	}
	o := w.attempt(tctx, a, t)
	if _, _, err := w.record(w.writeCtx(), t, o, nil, nil); err != nil {
		w.log.LogAttrs(ctx, slog.LevelError, "flows: activity outcome not recorded (it runs again after its lease expires)",
			slog.String("activity", t.Name), slog.Any("error", err))
	}
}

// record stores an attempt's outcome, retrying transient store errors a
// few times (a lost outcome would wait out the activity's lease); with
// resume it also resumes that workflow lease and returns the next workflow
// task. resumeLost reports a resume the store refused: the workflow lease
// is gone.
func (w *Worker) record(ctx context.Context, t *store.ActivityTask, o attemptOutcome, resume *store.Lease, into *store.WorkflowTask) (next *store.WorkflowTask, resumeLost bool, err error) {
	delay := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		next, resumeLost, err = w.recordOnce(ctx, t, o, resume, into)
		if err == nil || attempt == 4 || ctx.Err() != nil || !sleep(w.taskCtx(), delay) {
			return next, resumeLost, err
		}
		delay *= 2
	}
}

func (w *Worker) recordOnce(ctx context.Context, t *store.ActivityTask, o attemptOutcome, resume *store.Lease, into *store.WorkflowTask) (next *store.WorkflowTask, resumeLost bool, err error) {
	switch {
	case o.lost:
		return nil, false, nil
	case o.retry:
		err := w.st.RetryActivityTask(ctx, t.Lease, o.delay, o.maybe, o.keep)
		if err == nil && o.delay > 0 {
			// Measured after the commit: never before the store's time.
			w.actDue.add(time.Now().Add(o.delay))
		}
		if errors.Is(err, store.ErrLeaseLost) {
			err = nil
		}
		return nil, false, err
	}
	r := store.ActivityResult{Event: o.ev}
	if resume != nil {
		r.Resume, r.ResumeFor, r.Into = resume, w.opts.LeaseTTL, into
	}
	next, err = w.st.CompleteActivityTask(ctx, t.Lease, r)
	if errors.Is(err, store.ErrLeaseLost) {
		return nil, false, nil // another owner records the outcome; the workflow lease is untouched
	}
	if err != nil {
		return nil, false, err
	}
	return next, resume != nil && next == nil, nil
}
