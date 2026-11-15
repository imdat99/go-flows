package coro

// Future is a value that becomes available later: an activity result, a
// timer, a child workflow. The zero value is ready to use.
//
// Ported from go-workflows internal/sync/future.go (MIT, see NOTICE); waiting
// coroutines are now woken by Set instead of polling.
type Future[T any] struct {
	v       T
	err     error
	set     bool
	waiters waitList
}

// NewFuture returns an unset future.
func NewFuture[T any]() *Future[T] { return &Future[T]{} }

// Set stores the result and wakes every coroutine waiting on it. It reports
// false, and changes nothing, when the future was already set: a result is
// final (go-workflows overwrote it).
func (f *Future[T]) Set(v T, err error) bool {
	if f.set {
		return false
	}
	f.v, f.err, f.set = v, err, true
	f.waiters.wakeAll()
	return true
}

// Ready reports whether the future has been set.
func (f *Future[T]) Ready() bool { return f.set }

// Get blocks the running coroutine until the future is set.
func (f *Future[T]) Get(ctx Context) (T, error) {
	if !f.set {
		c := running(ctx)
		for !f.set {
			f.waiters.add(c)
			c.park()
		}
	}
	return f.v, f.err
}

// Peek returns the result without blocking; ok is false when not set.
func (f *Future[T]) Peek() (v T, err error, ok bool) {
	return f.v, f.err, f.set
}

func (f *Future[T]) register(c *Coroutine) { f.waiters.add(c) }
