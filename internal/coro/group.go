package coro

import "errors"

// ErrGroupMisuse reports Go or Add called after Wait returned, or a negative
// counter. go-workflows panicked; here it is an error the caller returns.
var ErrGroupMisuse = errors.New("coro: wait group misuse")

// WaitGroup waits for a set of coroutines.
//
// Ported from go-workflows internal/sync/waitgroup.go (MIT, see NOTICE).
type WaitGroup struct {
	n    int
	done Future[struct{}]
}

// Add adds delta to the counter.
func (wg *WaitGroup) Add(delta int) error {
	if wg.done.set && delta > 0 {
		return ErrGroupMisuse
	}
	wg.n += delta
	if wg.n < 0 {
		wg.n = 0
		return ErrGroupMisuse
	}
	return nil
}

// Done decrements the counter and releases Wait when it reaches zero.
func (wg *WaitGroup) Done() error {
	if err := wg.Add(-1); err != nil {
		return err
	}
	if wg.n == 0 {
		wg.done.Set(struct{}{}, nil)
	}
	return nil
}

// Wait blocks until the counter is zero.
func (wg *WaitGroup) Wait(ctx Context) {
	if wg.n == 0 {
		return
	}
	_, _ = wg.done.Get(ctx)
}

// ErrGroup runs coroutines and returns the first error; the first error
// cancels the group's context.
//
// Ported from go-workflows internal/sync/errgroup.go (MIT, see NOTICE).
type ErrGroup struct {
	ctx     Context
	cancel  CancelFunc
	n       int
	waiting bool
	err     error
	done    Future[struct{}]
}

// NewErrGroup returns a group and the context its coroutines get.
func NewErrGroup(parent Context) (*ErrGroup, Context) {
	ctx, cancel := WithCancel(parent)
	return &ErrGroup{ctx: ctx, cancel: cancel}, ctx
}

// Go runs fn in a new coroutine.
func (g *ErrGroup) Go(fn func(Context) error) error {
	if g.waiting {
		return ErrGroupMisuse
	}
	g.n++
	g.ctx.scheduler().Spawn(g.ctx, func(ctx Context) error {
		if err := fn(ctx); err != nil && g.err == nil {
			g.err = err
			g.cancel()
		}
		g.n--
		if g.n == 0 {
			g.done.Set(struct{}{}, nil)
		}
		return nil
	})
	return nil
}

// Wait blocks until every coroutine returned and returns the first error.
func (g *ErrGroup) Wait(ctx Context) error {
	g.waiting = true
	if g.n > 0 {
		_, _ = g.done.Get(ctx)
	}
	return g.err
}
