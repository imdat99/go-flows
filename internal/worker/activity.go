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
	lost  bool // the lease was lost: record nothing
}

// ErrUncertain is wrapped by an activity's error to mark a failure after
// which the activity may still have had its effect (its compensator runs).
var ErrUncertain = errors.New("outcome uncertain")

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
	var start time.Time
	if w.opts.Observer != nil {
		start = time.Now()
	}
	o := w.attemptOnce(ctx, a, t)
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
	if !t.Compensate && !o.lost && !o.retry && o.ev.Uncertain && len(o.ev.CompData) == 0 {
		if _, ok := w.acts.Activity(t.Name, true); ok {
			o.ev.CompData = nullData
		}
	}
	if t.Compensate && !o.lost && !o.retry {
		if o.ev.Type == history.ActivityCompleted {
			o.ev = history.Event{Type: history.CompensationCompleted}
		} else {
			o.ev = history.Event{Type: history.CompensationFailed, Err: o.ev.Err, ErrKinds: o.ev.ErrKinds, Permanent: o.ev.Permanent}
		}
	}
	return o
}

func (w *Worker) attemptOnce(ctx context.Context, a *Activity, t *store.ActivityTask) attemptOutcome {
	actx := ctx
	if a.Timeout > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeout(ctx, a.Timeout)
		defer cancel()
	}
	if t.CancelRequested {
		// Canceled by the workflow before this attempt: do not run it. An
		// earlier attempt (a retry is pending) may have had an effect; its
		// compensator gets the zero value. Before any attempt, certain.
		return attemptOutcome{err: ErrActivityCanceled, ev: history.Event{
			Type: history.ActivityFailed, Err: ErrActivityCanceled.Error(), ErrKinds: w.opts.Errors.Kinds(ErrActivityCanceled),
			Permanent: true, Uncertain: t.Attempt > 1,
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
		return attemptOutcome{retry: true, err: err}
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
		return attemptOutcome{retry: true, delay: a.Retry.Delay(t.Attempt + 1), err: err}
	}
	return attemptOutcome{err: err, ev: history.Event{
		Type: history.ActivityFailed, Err: w.opts.Errors.Text(err), ErrKinds: w.opts.Errors.Kinds(err),
		Permanent: permanent, Uncertain: uncertain, CompData: comp,
	}}
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
	tctx, untrack := w.hb.track(w.taskCtx(), true, t.Lease, claimed)
	defer untrack()
	a, ok := w.acts.Activity(t.Name, t.Compensate)
	if !ok {
		// Another instance may have it: give the task back for a while.
		if err := w.st.RetryActivityTask(tctx, t.Lease, w.opts.HandBack); err != nil && !errors.Is(err, store.ErrLeaseLost) {
			w.log.LogAttrs(ctx, slog.LevelWarn, "flows: hand back activity", slog.String("activity", t.Name), slog.Any("error", err))
		}
		return
	}
	o := w.attempt(tctx, a, t)
	if _, _, err := w.record(context.WithoutCancel(tctx), t, o, nil); err != nil {
		w.log.LogAttrs(ctx, slog.LevelWarn, "flows: record activity outcome",
			slog.String("activity", t.Name), slog.Any("error", err))
	}
}

// record stores an attempt's outcome, retrying transient store errors a
// few times (a lost outcome would wait out the activity's lease); with
// resume it also resumes that workflow lease and returns the next workflow
// task. resumeLost reports a resume the store refused: the workflow lease
// is gone.
func (w *Worker) record(ctx context.Context, t *store.ActivityTask, o attemptOutcome, resume *store.Lease) (next *store.WorkflowTask, resumeLost bool, err error) {
	delay := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		next, resumeLost, err = w.recordOnce(ctx, t, o, resume)
		if err == nil || attempt == 4 || !sleep(w.taskCtx(), delay) {
			return next, resumeLost, err
		}
		delay *= 2
	}
}

func (w *Worker) recordOnce(ctx context.Context, t *store.ActivityTask, o attemptOutcome, resume *store.Lease) (next *store.WorkflowTask, resumeLost bool, err error) {
	switch {
	case o.lost:
		return nil, false, nil
	case o.retry:
		err := w.st.RetryActivityTask(ctx, t.Lease, o.delay)
		if errors.Is(err, store.ErrLeaseLost) {
			err = nil
		}
		return nil, false, err
	}
	r := store.ActivityResult{Event: o.ev}
	if resume != nil {
		r.Resume, r.ResumeFor = resume, w.opts.LeaseTTL
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
