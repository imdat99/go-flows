package coro

// Case is one alternative of Select.
//
// Ported in spirit from go-workflows internal/sync/selector.go (MIT, see
// NOTICE); cases now register as waiters instead of being polled.
type Case interface {
	ready() bool
	register(c *Coroutine)
	fire(ctx Context)
}

// OnFuture fires when f is set.
func OnFuture[T any](f *Future[T], fn func(ctx Context, v T, err error)) Case {
	return &futureCase[T]{f: f, fn: fn}
}

// OnReceive fires when ch has a value or is closed (ok == false).
func OnReceive[T any](ch *Channel[T], fn func(ctx Context, v T, ok bool)) Case {
	return &receiveCase[T]{ch: ch, fn: fn}
}

// OnDefault fires when no other case is ready.
func OnDefault(fn func(ctx Context)) Case { return &defaultCase{fn: fn} }

// Select blocks until one case is ready and fires it. Cases are checked in
// order, so the first ready case wins (deterministic). With an OnDefault
// case, Select never blocks.
func Select(ctx Context, cases ...Case) {
	var def Case
	for {
		for _, k := range cases {
			if d, ok := k.(*defaultCase); ok {
				def = d
				continue
			}
			if k.ready() {
				k.fire(ctx)
				return
			}
		}
		if def != nil {
			def.fire(ctx)
			return
		}
		c := running(ctx)
		for _, k := range cases {
			k.register(c)
		}
		c.park()
	}
}

type futureCase[T any] struct {
	f  *Future[T]
	fn func(Context, T, error)
}

func (k *futureCase[T]) ready() bool           { return k.f.set }
func (k *futureCase[T]) register(c *Coroutine) { k.f.register(c) }
func (k *futureCase[T]) fire(ctx Context)      { k.fn(ctx, k.f.v, k.f.err) }

type receiveCase[T any] struct {
	ch *Channel[T]
	fn func(Context, T, bool)
}

func (k *receiveCase[T]) ready() bool           { return k.ch.canReceive() }
func (k *receiveCase[T]) register(c *Coroutine) { k.ch.recvWait.add(c) }
func (k *receiveCase[T]) fire(ctx Context) {
	v, ok, _ := k.ch.TryReceive()
	k.fn(ctx, v, ok)
}

type defaultCase struct{ fn func(Context) }

func (*defaultCase) ready() bool         { return true }
func (*defaultCase) register(*Coroutine) {}
func (k *defaultCase) fire(ctx Context)  { k.fn(ctx) }
