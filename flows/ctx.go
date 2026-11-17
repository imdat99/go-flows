package flows

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/text"
)

// Ctx is the context of workflow code. Workflow code runs deterministically
// and is replayed: it must not do I/O, read the clock, start goroutines or
// use randomness itself. It calls activities for side effects, and Now,
// Rand, SideEffect, Go and Select instead of the standard ones.
type Ctx struct{ c coro.Context }

func (c Ctx) state() *executor.State {
	if c.c == nil {
		panic(ErrNotInWorkflow)
	}
	st := executor.FromContext(c.c)
	if st == nil {
		panic(ErrNotInWorkflow)
	}
	return st
}

// inner returns c's coroutine context, panicking with ErrNotInWorkflow for
// a zero Ctx.
func (c Ctx) inner() coro.Context {
	if c.c == nil {
		panic(ErrNotInWorkflow)
	}
	return c.c
}

// Err reports ErrCanceled once the context is canceled (the workflow was
// canceled, or a WithCancel parent was).
func (c Ctx) Err() error {
	if c.c == nil {
		return ErrNotInWorkflow
	}
	return c.c.Err()
}

// Value returns the value stored under key (WithValue), or nil.
func (c Ctx) Value(key any) any { return c.inner().Value(key) }

// WithValue returns a child of c carrying val under key.
func WithValue(c Ctx, key, val any) Ctx { return Ctx{coro.WithValue(c.inner(), key, val)} }

// WithCancel returns a child of c canceled by cancel or with c. Canceling
// the context of a running activity, timer or child cancels it, and its
// future resolves with ErrCanceled at once (for a child whose real outcome
// matters, see Future.RequestCancel).
func WithCancel(c Ctx) (Ctx, func()) {
	cc, cancel := coro.WithCancel(c.inner())
	return Ctx{cc}, cancel
}

// Disconnected returns a context with c's values that is not canceled with
// c: for cleanup a canceled workflow still does.
func Disconnected(c Ctx) (Ctx, func()) {
	cc, cancel := coro.Disconnected(c.inner())
	return Ctx{cc}, cancel
}

// Go runs fn concurrently with the workflow, deterministically (a
// coroutine, not a goroutine).
func Go(c Ctx, fn func(c Ctx)) {
	coro.Go(c.inner(), func(cc coro.Context) { fn(Ctx{cc}) })
}

// Now returns the workflow time: the database time of the current task's
// start, the same on every replay.
func Now(c Ctx) time.Time { return c.state().Now() }

// InstanceID returns the id of the running workflow instance: the id it
// was started with (Handle.InstanceID, RunInfo.InstanceID), or the id its
// parent gave a child; not the engine's Options.InstanceID. Continue-as-new
// keeps it. It is the same on every replay, so it may go into activity
// inputs or idempotency keys.
func InstanceID(c Ctx) string { return c.state().InstanceID() }

// ExecutionID returns the id of the running execution: one run of the
// instance, unique in its store (WorkflowTaskInfo.ExecutionID,
// ActivityInfo.ExecutionID). Every replay of the execution sees the same
// id; continue-as-new starts an execution with a new one.
func ExecutionID(c Ctx) int64 { return c.state().ExecutionID() }

// Replaying reports whether the workflow is replaying recorded history;
// use it to skip logging or metrics on replay.
func Replaying(c Ctx) bool { return c.state().Replaying() }

// Sleep waits d of workflow time (a durable timer: the worker does not
// hold the workflow while it waits). It returns ErrCanceled if c is
// canceled first.
func Sleep(c Ctx, d time.Duration) error {
	_, err := NewTimer(c, d).Get(c)
	return err
}

// NewTimer starts a durable timer firing d after the current workflow time.
func NewTimer(c Ctx, d time.Duration) *Future[struct{}] {
	f := c.state().NewTimer(c.c, d, "")
	return &Future[struct{}]{timer: f}
}

// SideEffect records fn's value the first time the workflow reaches it and
// returns the recorded value on every replay: for a value computed outside
// the workflow's determinism (a generated id). A value that cannot be
// encoded fails the workflow (a panic, recorded with ErrPanic): returning an
// error live would not replay.
func SideEffect[T any](c Ctx, fn func() T) (T, error) {
	cd := newCodec[T]()
	raw, err := c.state().SideEffect(c.c, func() []byte {
		b, err := cd.encode(fn())
		if err != nil {
			panic(Permanent(fmt.Errorf("flows: side effect value not encodable: %w", err)))
		}
		return b
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return cd.decode(raw)
}

// Rand returns a random number, recorded (SideEffect).
func Rand(c Ctx) (uint64, error) {
	raw, err := c.state().SideEffect(c.c, func() []byte {
		return binary.BigEndian.AppendUint64(nil, rand.Uint64()) //nolint:gosec // not for security
	})
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("%w: recorded random value has %d bytes", ErrNonDeterministic, len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

// Future is the result of an activity, a child workflow or a timer.
type Future[T any] struct {
	res   *coro.Future[executor.Result]
	timer *coro.Future[struct{}]
	cd    codec[T]
	err   error // failed before issuing (input not encodable)
}

var errSelectNoCases = errors.New("flows: Select without cases")

var errZeroFuture = errors.New("flows: zero Future (use the one returned by Start, Child or NewTimer)")

// Get waits for the result.
func (f *Future[T]) Get(c Ctx) (T, error) {
	var zero T
	switch {
	case f.err != nil:
		return zero, f.err
	case f.timer != nil:
		_, err := f.timer.Get(c.c)
		return zero, err
	case f.res == nil:
		return zero, errZeroFuture
	}
	r, err := f.res.Get(c.c)
	if err != nil {
		return zero, err
	}
	if r.Err != nil {
		return zero, fromExecutor(r.Err)
	}
	return f.cd.decode(r.Output)
}

// Ready reports whether Get would return without waiting.
func (f *Future[T]) Ready() bool {
	switch {
	case f.err != nil:
		return true
	case f.timer != nil:
		return f.timer.Ready()
	case f.res == nil:
		return true
	}
	return f.res.Ready()
}

// RequestCancel asks the child workflow f is the future of (Child) to
// cancel, and leaves f to resolve with the child's real outcome: its
// output if it completes anyway (it may have passed a point of no return,
// or ignore the cancel), or its error when it fails or honours the cancel
// (errors.Is ErrCanceled). The child sees the request as a canceled root
// context (c.Err), and decides: stop, or finish what must be finished on a
// Disconnected context. Canceling the context f was started with instead
// resolves f with ErrCanceled at once, without the child's outcome.
//
// The request is recorded in the history and reaches the child with the
// checkpoint that records it. A child started in the same task (not
// checkpointed yet) is not started at all: f resolves with ErrCanceled. A
// repeated request, or one after the child ended, does nothing; so does a
// request on a future that failed before the child was started. A child
// that completes after the request keeps its compensations: it is
// compensated with its parent as any completed child. A child that
// continues as new after the request starts its next run uncanceled (the
// request is not carried over): the parent waits for the whole chain.
// RequestCancel on the
// future of an activity or a timer panics, failing the workflow
// (ErrInvalidArgument).
func (f *Future[T]) RequestCancel(c Ctx) {
	st := c.state()
	switch {
	case f.err != nil:
		return
	case f.timer != nil || f.res == nil:
		panic(Permanent(fmt.Errorf("%w: RequestCancel on a future that is not a child workflow's", ErrInvalidArgument)))
	}
	if err := st.RequestCancelChild(f.res); err != nil {
		panic(Permanent(fmt.Errorf("%w: %w", ErrInvalidArgument, err)))
	}
}

// SelectCase is one alternative of Select.
type SelectCase struct{ c func() coro.Case }

// OnFuture fires when f has its result.
func OnFuture[T any](f *Future[T], fn func(c Ctx, v T, err error)) SelectCase {
	return SelectCase{func() coro.Case {
		switch {
		case f.err != nil:
			done := coro.NewFuture[struct{}]()
			done.Set(struct{}{}, nil)
			return coro.OnFuture(done, func(cc coro.Context, _ struct{}, _ error) {
				var zero T
				fn(Ctx{cc}, zero, f.err)
			})
		case f.timer != nil:
			return coro.OnFuture(f.timer, func(cc coro.Context, _ struct{}, err error) {
				var zero T
				fn(Ctx{cc}, zero, err)
			})
		case f.res == nil:
			f.err = errZeroFuture
			return OnFuture(f, fn).c()
		}
		return coro.OnFuture(f.res, func(cc coro.Context, _ executor.Result, _ error) {
			v, err := f.Get(Ctx{cc})
			fn(Ctx{cc}, v, err)
		})
	}}
}

// OnSignal fires when a signal arrives on ch.
func OnSignal[T any](ch *SignalChannel[T], fn func(c Ctx, v T, err error)) SelectCase {
	return SelectCase{func() coro.Case {
		return coro.OnReceive(ch.ch, func(cc coro.Context, raw []byte, _ bool) {
			v, err := ch.cd.decode(raw)
			fn(Ctx{cc}, v, err)
		})
	}}
}

// OnDefault fires when no other case is ready (Select does not block).
func OnDefault(fn func(c Ctx)) SelectCase {
	return SelectCase{func() coro.Case { return coro.OnDefault(func(cc coro.Context) { fn(Ctx{cc}) }) }}
}

// Select waits until one case is ready and runs it; the first ready case
// in order wins (deterministic). A Select without cases (or with only
// zero ones) would block forever: it panics, failing the workflow.
func Select(c Ctx, cases ...SelectCase) {
	cc := make([]coro.Case, 0, len(cases))
	for _, k := range cases {
		if k.c != nil { // a zero SelectCase is ignored
			cc = append(cc, k.c())
		}
	}
	if len(cc) == 0 {
		panic(Permanent(errSelectNoCases))
	}
	coro.Select(c.inner(), cc...)
}

// SignalChannel receives the signals sent to the workflow under one name,
// in arrival order; none is dropped.
type SignalChannel[T any] struct {
	ch *coro.Channel[[]byte]
	cd codec[T]
}

// Signals returns the channel of the signals called name. An invalid name
// (see Engine.Signal) fails the workflow.
func Signals[T any](c Ctx, name string) *SignalChannel[T] {
	if err := text.CheckName(ErrInvalidArgument, "signal name", name); err != nil {
		panic(Permanent(err))
	}
	return &SignalChannel[T]{ch: c.state().SignalChannel(name), cd: newCodec[T]()}
}

// Receive waits for the next signal; it returns ErrCanceled when c is
// canceled first.
func (s *SignalChannel[T]) Receive(c Ctx) (T, error) {
	var zero T
	done := c.inner().Done()
	if done == nil {
		raw, _ := s.ch.Receive(c.c)
		return s.cd.decode(raw)
	}
	var (
		raw      []byte
		canceled bool
	)
	coro.Select(c.c,
		coro.OnReceive(s.ch, func(_ coro.Context, v []byte, _ bool) { raw = v }),
		coro.OnReceive(done, func(coro.Context, struct{}, bool) { canceled = true }),
	)
	if canceled {
		return zero, ErrCanceled
	}
	return s.cd.decode(raw)
}

// TryReceive returns the next signal if one is buffered.
func (s *SignalChannel[T]) TryReceive() (v T, ok bool, err error) {
	raw, ok, done := s.ch.TryReceive()
	if !done || !ok {
		return v, false, nil
	}
	v, err = s.cd.decode(raw)
	return v, true, err
}

// Await waits for the next signal called name, at most timeout (0: no
// limit). It returns ErrAwaitTimeout when the time is up, ErrCanceled when
// c is canceled.
func Await[T any](c Ctx, name string, timeout time.Duration) (T, error) {
	ch := Signals[T](c, name)
	if timeout <= 0 {
		return ch.Receive(c)
	}
	tc, cancelTimer := WithCancel(c)
	defer cancelTimer()
	timer := NewTimer(tc, timeout)
	var (
		v   T
		err error
	)
	Select(c,
		OnSignal(ch, func(_ Ctx, got T, e error) { v, err = got, e }),
		OnFuture(timer, func(_ Ctx, _ struct{}, e error) {
			if err = e; err == nil {
				err = ErrAwaitTimeout
			}
		}),
	)
	return v, err
}

// WaitGroup waits for coroutines started with Go, deterministically (a
// sync.WaitGroup would block the workflow). The zero value is ready, and it
// can be reused once its counter reached zero and its waiters returned (as
// with sync.WaitGroup, an Add from zero while a Wait is returning makes it
// wait again). A negative counter panics, failing the workflow.
type WaitGroup struct{ wg coro.WaitGroup }

// Add adds n to the counter.
func (w *WaitGroup) Add(n int) {
	if err := w.wg.Add(n); err != nil {
		panic(Permanent(fmt.Errorf("flows: WaitGroup.Add(%d): %w", n, err)))
	}
}

// Done decrements the counter.
func (w *WaitGroup) Done() {
	if err := w.wg.Done(); err != nil {
		panic(Permanent(fmt.Errorf("flows: WaitGroup.Done: %w", err)))
	}
}

// Wait waits until the counter is zero (nil), or c is canceled first
// (ErrCanceled; the coroutines run on).
func (w *WaitGroup) Wait(c Ctx) error {
	if w.wg.WaitCtx(c.inner()) {
		return nil
	}
	return c.Err()
}
