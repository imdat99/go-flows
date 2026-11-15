package coro

import "errors"

// ErrChannelClosed is returned by Send on a closed channel. A send on a
// closed Go channel panics; workflow code gets an error instead (GW-10).
var ErrChannelClosed = errors.New("coro: send on closed channel")

// Channel is a deterministic channel between coroutines.
//
// Capacity semantics:
//   - capacity 0 is unbuffered: Send completes when a receiver takes the
//     value;
//   - a positive capacity buffers that many values;
//   - a negative capacity is unbounded. Signals use it, so that a value is
//     never dropped (GW-14).
//
// Every change a receiver could act on wakes all parked receivers, which
// re-check in FIFO order. With Select a woken receiver may take another
// case, so waking a single receiver could lose the wake-up. Receivers per
// channel are few, so this is cheap.
//
// Ported from go-workflows internal/sync/channel.go (MIT, see NOTICE),
// rewritten with wake-ups and allocation-free queues.
type Channel[T any] struct {
	buf    queue[T]
	cap    int
	closed bool

	// Blocked senders, FIFO. Each offers one value; a receiver takes it and
	// marks the offer taken. Offers are recycled through free.
	senders queue[*offer[T]]
	free    []*offer[T]

	recvWait waitList // receivers blocked on nothing to receive
	sendWait waitList // senders blocked until their offer is taken
}

type offer[T any] struct {
	v      T
	taken  bool // a receiver took v (or moved it into the buffer)
	closed bool // the channel was closed before v was taken
}

// NewChannel returns a channel with the given capacity (see Channel).
func NewChannel[T any](capacity int) *Channel[T] { return &Channel[T]{cap: capacity} }

// Len returns the number of buffered values.
func (ch *Channel[T]) Len() int { return ch.buf.len() }

// Closed reports whether Close was called.
func (ch *Channel[T]) Closed() bool { return ch.closed }

// Close closes the channel. Buffered values can still be received, then
// receivers get ok == false. Senders blocked at Close get ErrChannelClosed,
// and their values are not delivered. Closing twice is a no-op.
func (ch *Channel[T]) Close() {
	if ch.closed {
		return
	}
	ch.closed = true
	for ch.senders.len() > 0 {
		if o := ch.senders.pop(); !o.taken {
			o.closed = true
		}
	}
	ch.recvWait.wakeAll()
	ch.sendWait.wakeAll()
}

func (ch *Channel[T]) hasRoom() bool { return ch.cap < 0 || ch.buf.len() < ch.cap }

// Send sends v, blocking until it is buffered or taken by a receiver.
func (ch *Channel[T]) Send(ctx Context, v T) error {
	if ok, err := ch.TrySend(v); ok || err != nil {
		return err
	}
	c := running(ctx)
	o := ch.newOffer(v)
	ch.senders.push(o)
	ch.recvWait.wakeAll()
	for !o.taken && !o.closed {
		ch.sendWait.add(c)
		c.park()
	}
	closed := o.closed && !o.taken
	ch.recycle(o)
	if closed {
		return ErrChannelClosed
	}
	return nil
}

// TrySend buffers v without blocking and reports whether it did. On an
// unbuffered or full channel it fails even if a receiver is parked. Workflow
// code uses Send for rendezvous; signals use unbounded channels.
func (ch *Channel[T]) TrySend(v T) (bool, error) {
	if ch.closed {
		return false, ErrChannelClosed
	}
	if !ch.hasRoom() {
		return false, nil
	}
	ch.buf.push(v)
	ch.recvWait.wakeAll()
	return true, nil
}

// Receive blocks until a value is available or the channel is closed and
// drained (ok == false).
func (ch *Channel[T]) Receive(ctx Context) (v T, ok bool) {
	for {
		if v, ok, done := ch.TryReceive(); done {
			return v, ok
		}
		c := running(ctx)
		ch.recvWait.add(c)
		c.park()
	}
}

// TryReceive receives without blocking. done reports whether the call
// completed: a value, or closed and drained with ok == false.
func (ch *Channel[T]) TryReceive() (v T, ok, done bool) {
	if ch.buf.len() > 0 {
		v = ch.buf.pop()
		// Room again: the oldest blocked sender's value moves in.
		if ch.hasRoom() {
			if o := ch.takeOffer(); o != nil {
				ch.buf.push(o.v)
			}
		}
		return v, true, true
	}
	if o := ch.takeOffer(); o != nil {
		return o.v, true, true
	}
	if ch.closed {
		return v, false, true
	}
	return v, false, false
}

func (ch *Channel[T]) canReceive() bool {
	return ch.buf.len() > 0 || ch.senders.len() > 0 || ch.closed
}

// takeOffer takes the oldest pending offer of a blocked sender and wakes the
// blocked senders. Its owner is among them; the others park again.
func (ch *Channel[T]) takeOffer() *offer[T] {
	for ch.senders.len() > 0 {
		o := ch.senders.pop()
		if o.taken || o.closed {
			continue
		}
		o.taken = true
		ch.sendWait.wakeAll()
		return o
	}
	return nil
}

func (ch *Channel[T]) newOffer(v T) *offer[T] {
	if n := len(ch.free); n > 0 {
		o := ch.free[n-1]
		ch.free = ch.free[:n-1]
		o.v = v
		return o
	}
	return &offer[T]{v: v}
}

// recycle returns an offer once its sender has returned. The offer is no
// longer in the senders queue: it was popped when taken or at Close.
func (ch *Channel[T]) recycle(o *offer[T]) {
	*o = offer[T]{}
	ch.free = append(ch.free, o)
}

// queue is a FIFO over a slice that reuses its backing array, so steady-state
// push and pop do not allocate.
type queue[T any] struct {
	s    []T
	head int
}

func (q *queue[T]) len() int { return len(q.s) - q.head }

func (q *queue[T]) push(v T) {
	if q.head > 0 && q.head == len(q.s) {
		q.s = q.s[:0]
		q.head = 0
	} else if q.head > 0 && len(q.s) == cap(q.s) && q.head >= len(q.s)/2 {
		n := copy(q.s, q.s[q.head:])
		clear(q.s[n:])
		q.s = q.s[:n]
		q.head = 0
	}
	q.s = append(q.s, v)
}

func (q *queue[T]) pop() T {
	v := q.s[q.head]
	var zero T
	q.s[q.head] = zero
	q.head++
	return v
}
