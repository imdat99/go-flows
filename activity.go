package flows

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/worker"
)

// Key identifies one activity of one workflow run: "instanceID:scheduleID:
// name". It is the same on every attempt and replay of that activity, so
// it is the idempotency key the activity's side effects should be deduplicated
// by (activities run at least once).
type Key string

func activityKey(t *store.ActivityTask) Key {
	return Key(t.InstanceID + ":" + strconv.Itoa(int(t.ScheduleID)) + ":" + t.Name)
}

// InvokeFunc runs an activity: its output, and the data its compensator
// needs to undo it.
type InvokeFunc[I, O, C any] func(ctx context.Context, k Key, in I) (O, C, error)

// CompensateFunc undoes an activity from its compensation data. It runs at
// least once, possibly for an activity that did not take effect
// (CompensationUncertain), and must be idempotent. Compensators run when the
// workflow fails or is canceled, for every activity that completed or may
// have had its effect, one at a time, latest first.
type CompensateFunc[C any] func(ctx context.Context, k Key, comp C) error

type uncertainKey struct{}

// CompensationUncertain reports, in a compensator, that the activity it
// undoes failed in a way that may or may not have had its effect (a crash,
// a timeout, a panic, a cancel): its compensation data may be the zero
// value, and there may be nothing to undo.
func CompensationUncertain(ctx context.Context) bool {
	v, _ := ctx.Value(uncertainKey{}).(bool)
	return v
}

// RetryPolicy decides whether a failed attempt is retried and after how
// long: Attempts in total (the first included), Initial delay growing by
// Factor up to Max. Retries happen on the activity task and are not
// recorded in the workflow's history, so changing a policy is safe.
type RetryPolicy = worker.RetryPolicy

// Exponential returns a policy of attempts tries, starting at initial and
// doubling up to maxDelay.
func Exponential(attempts int, initial, maxDelay time.Duration) RetryPolicy {
	return RetryPolicy{Attempts: attempts, Initial: initial, Factor: 2, Max: maxDelay}
}

// DefaultRetry is the policy of activities defined without one.
var DefaultRetry = Exponential(5, time.Second, time.Minute)

// ActivityOption configures an activity.
type ActivityOption func(*activityConfig)

type activityConfig struct {
	retry, compRetry RetryPolicy
	timeout          time.Duration
	queue            string
}

// WithRetry sets the retry policy of the activity's attempts.
func WithRetry(p RetryPolicy) ActivityOption { return func(c *activityConfig) { c.retry = p } }

// WithCompensateRetry sets the retry policy of the compensator.
func WithCompensateRetry(p RetryPolicy) ActivityOption {
	return func(c *activityConfig) { c.compRetry = p }
}

// WithTimeout bounds each attempt; an attempt that times out failed but is
// uncertain (it may have had its effect).
func WithTimeout(d time.Duration) ActivityOption { return func(c *activityConfig) { c.timeout = d } }

// WithQueue runs the activity on queue (workers poll their queues).
func WithQueue(queue string) ActivityOption { return func(c *activityConfig) { c.queue = queue } }

// Activity is an activity definition: a side-effecting call made by
// workflows, run by workers at least once, optionally with a compensator.
type Activity[I, O, C any] struct {
	name  string
	queue string
	in    codec[I]
	out   codec[O]
	cfg   activityConfig
}

// DefineActivity defines an activity called name (stable: it is recorded).
// compensate may be nil: then the compensation data (C) is not stored. With
// a compensator, the data returned with the output (or with a failure, for
// an attempt that may have had its effect) is stored and given to it.
// Define activities at package level, in every binary that runs them.
func DefineActivity[I, O, C any](name string, invoke InvokeFunc[I, O, C], compensate CompensateFunc[C], opts ...ActivityOption) *Activity[I, O, C] {
	cfg := activityConfig{retry: DefaultRetry, compRetry: DefaultRetry, queue: DefaultQueue}
	for _, o := range opts {
		o(&cfg)
	}
	a := &Activity[I, O, C]{name: name, queue: cfg.queue, in: newCodec[I](), out: newCodec[O](), cfg: cfg}
	var compensator *worker.Activity
	if compensate != nil {
		compensator = a.compensator(compensate)
	}
	registerActivity(name, a.invoker(invoke, compensate != nil), compensator)
	return a
}

// invoker runs invoke for an activity task; withComp stores its
// compensation data.
func (a *Activity[I, O, C]) invoker(invoke InvokeFunc[I, O, C], withComp bool) *worker.Activity {
	name, comp := a.name, newCodec[C]()
	return &worker.Activity{
		Retry: a.cfg.retry, Timeout: a.cfg.timeout,
		Run: func(ctx context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
			in, err := a.in.decode(t.Input)
			if err != nil {
				return nil, nil, Permanent(fmt.Errorf("decode input of %s: %w", name, err))
			}
			out, c, err := invoke(ctx, activityKey(t), in)
			if errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				err = fmt.Errorf("%w: %w", ErrTimeout, err)
			}
			var cb []byte
			if withComp { // without a compensator nothing is stored
				var cerr error
				if cb, cerr = comp.encode(c); cerr != nil {
					if err != nil {
						return nil, nil, err // the activity's own error tells more
					}
					// It took effect, but cannot be undone with its data: an
					// uncertain failure (the compensator gets the zero value).
					return nil, nil, Permanent(fmt.Errorf("%w: encode compensation data of %s: %w", worker.ErrUncertain, name, cerr))
				}
			}
			if err != nil {
				return nil, cb, err
			}
			ob, err := a.out.encode(out)
			if err != nil {
				return nil, cb, Permanent(fmt.Errorf("encode output of %s: %w", name, err))
			}
			return ob, cb, nil
		},
	}
}

// compensator runs compensate for a compensation task.
func (a *Activity[I, O, C]) compensator(compensate CompensateFunc[C]) *worker.Activity {
	name, comp := a.name, newCodec[C]()
	return &worker.Activity{
		Retry: a.cfg.compRetry, Timeout: a.cfg.timeout,
		Run: func(ctx context.Context, t *store.ActivityTask) ([]byte, []byte, error) {
			c, err := comp.decode(t.Input)
			if err != nil {
				return nil, nil, Permanent(fmt.Errorf("decode compensation data of %s: %w", name, err))
			}
			if t.Uncertain {
				ctx = context.WithValue(ctx, uncertainKey{}, true)
			}
			return nil, nil, compensate(ctx, activityKey(t), c)
		},
	}
}

// Name returns the activity's name.
func (a *Activity[I, O, C]) Name() string { return a.name }

// Start schedules the activity from workflow code and returns its future.
// Canceling c (WithCancel) cancels the activity: its future resolves with
// ErrCanceled, and the running attempt's context is canceled.
func (a *Activity[I, O, C]) Start(c Ctx, in I) *Future[O] {
	b, err := a.in.encode(in)
	if err != nil {
		return &Future[O]{err: Permanent(fmt.Errorf("encode input of %s: %w", a.name, err))}
	}
	return &Future[O]{res: c.state().ScheduleActivity(c.c, a.name, a.queue, b), cd: a.out}
}

// Run schedules the activity and waits for its result.
func (a *Activity[I, O, C]) Run(c Ctx, in I) (O, error) { return a.Start(c, in).Get(c) }
