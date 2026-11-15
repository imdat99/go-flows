package coro

import (
	"errors"
	"slices"
)

// ErrCanceled is the error of a canceled context.
var ErrCanceled = errors.New("context canceled")

// Context carries cancellation and values through workflow code. It is the
// deterministic counterpart of context.Context: Done is a coroutine channel.
//
// Ported from go-workflows internal/sync/context.go (MIT, see NOTICE),
// itself adapted from the standard library.
type Context interface {
	// Done is closed when the context is canceled; nil if it never is.
	Done() *Channel[struct{}]
	// Err is nil until canceled, then ErrCanceled.
	Err() error
	// Value returns the value stored under key, or nil.
	Value(key any) any

	scheduler() *Scheduler
}

type rootCtx struct{ s *Scheduler }

func (*rootCtx) Done() *Channel[struct{}] { return nil }
func (*rootCtx) Err() error               { return nil }
func (*rootCtx) Value(any) any            { return nil }
func (r *rootCtx) scheduler() *Scheduler  { return r.s }

// CancelFunc cancels a context.
type CancelFunc func()

// CancelCauseFunc cancels a context with a cause (see Cause).
type CancelCauseFunc func(cause error)

type cancelCtx struct {
	Context
	parent   *cancelCtx // linked parent, nil if none or disconnected
	done     *Channel[struct{}]
	children []*cancelCtx
	err      error
	cause    error
}

type cancelCtxKey struct{}

// WithCancel returns a child of parent canceled by the returned function or
// when parent is canceled.
func WithCancel(parent Context) (Context, CancelFunc) {
	c := newCancelCtx(parent, true)
	return c, func() { c.cancel(ErrCanceled, nil, true) }
}

// WithCancelCause is WithCancel with a cause reported by Cause.
func WithCancelCause(parent Context) (Context, CancelCauseFunc) {
	c := newCancelCtx(parent, true)
	return c, func(cause error) { c.cancel(ErrCanceled, cause, true) }
}

// Disconnected returns a context with parent's values that is not canceled
// with parent (cleanup after cancellation, compensation).
func Disconnected(parent Context) (Context, CancelFunc) {
	c := newCancelCtx(parent, false)
	return c, func() { c.cancel(ErrCanceled, nil, false) }
}

func newCancelCtx(parent Context, link bool) *cancelCtx {
	c := &cancelCtx{Context: parent, done: NewChannel[struct{}](0)}
	if !link {
		return c
	}
	if p, ok := parent.Value(cancelCtxKey{}).(*cancelCtx); ok {
		if p.err != nil {
			c.cancel(p.err, p.cause, false)
		} else {
			c.parent = p
			p.children = append(p.children, c)
		}
	}
	return c
}

func (c *cancelCtx) Value(key any) any {
	if _, ok := key.(cancelCtxKey); ok {
		return c
	}
	return c.Context.Value(key)
}

func (c *cancelCtx) Done() *Channel[struct{}] { return c.done }
func (c *cancelCtx) Err() error               { return c.err }

// cancel cancels c and its children. unlink removes c from its parent's
// children (a cancel function called directly; children canceled by their
// parent need not), so a loop of WithCancel+cancel does not grow the parent.
func (c *cancelCtx) cancel(err, cause error, unlink bool) {
	if c.err != nil {
		return
	}
	if cause == nil {
		cause = err
	}
	c.err, c.cause = err, cause
	c.done.Close()
	for _, child := range c.children {
		child.parent = nil
		child.cancel(err, cause, false)
	}
	c.children = nil
	if p := c.parent; unlink && p != nil {
		for i, x := range p.children {
			if x == c {
				p.children = slices.Delete(p.children, i, i+1) // zeroes the tail
				break
			}
		}
		c.parent = nil
	}
}

// Cause returns the cause given to the cancel function of the nearest
// cancelable context, its error when canceled without a cause, or nil.
func Cause(ctx Context) error {
	if c, ok := ctx.Value(cancelCtxKey{}).(*cancelCtx); ok {
		return c.cause
	}
	return nil
}

type valueCtx struct {
	Context
	key, val any
}

// WithValue returns a child of parent carrying val under key (a comparable
// value, typically an unexported struct type).
func WithValue(parent Context, key, val any) Context {
	return &valueCtx{Context: parent, key: key, val: val}
}

func (c *valueCtx) Value(key any) any {
	if c.key == key {
		return c.val
	}
	return c.Context.Value(key)
}

// Go spawns fn as a new coroutine of ctx's scheduler. A panic in fn becomes
// the scheduler's error (GW-10).
func Go(ctx Context, fn func(Context)) {
	ctx.scheduler().Spawn(ctx, func(ctx Context) error {
		fn(ctx)
		return nil
	})
}
