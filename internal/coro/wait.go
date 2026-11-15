package coro

// waitList holds coroutines parked on one primitive (a future, a channel
// side). A coroutine's gen is odd while it is parked and is incremented both
// when it parks and when it resumes, so an entry is valid only during the
// one park it was registered for: after the coroutine resumes, entries left
// on other primitives (a Select over several) are stale and skipped. Stale entries are
// pruned whenever a coroutine registers, so a Select loop over a primitive
// that never fires does not grow its list.
//
// The first waiter is stored inline (buf): a future or channel side almost
// always has one waiter, which then costs no allocation. A waitList must
// not be copied once used.
type waitList struct {
	w   []waiter
	buf [1]waiter
}

type waiter struct {
	c   *Coroutine
	gen uint32
}

// valid reports whether the coroutine is still parked on this registration.
func (x waiter) valid() bool { return x.c.gen == x.gen && !x.c.done }

// add registers c for its next park (park increments gen, so the entry
// records gen+1).
func (l *waitList) add(c *Coroutine) {
	if l.w == nil {
		l.w = l.buf[:0]
	}
	n := 0
	for _, x := range l.w {
		if x.valid() || x.gen == x.c.gen+1 { // parked on it, or about to park
			l.w[n] = x
			n++
		}
	}
	clear(l.w[n:])
	l.w = append(l.w[:n], waiter{c: c, gen: c.gen + 1})
}

// wakeAll moves every valid waiter to its scheduler's ready queue.
func (l *waitList) wakeAll() {
	for i, x := range l.w {
		if x.valid() {
			x.c.s.enqueue(x.c)
		}
		l.w[i] = waiter{}
	}
	l.w = l.w[:0]
}
