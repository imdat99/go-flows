// Package coro is the deterministic cooperative runtime that workflow code
// runs on: coroutines, futures, channels, select and a cancelable context.
//
// Ported from go-workflows internal/sync (MIT, see NOTICE) and rewritten
// around a ready queue:
//
//   - Only one coroutine of a Scheduler runs at a time, so the primitives need
//     no locks.
//   - A blocking operation parks the running coroutine on a wait list.
//     Whatever unblocks it (a future being set, a channel send) moves it to the
//     ready queue.
//   - A parking coroutine hands control directly to the next ready one in
//     FIFO order: one goroutine switch per hand-off, deterministic order.
//   - A run costs O(coroutines that can progress). go-workflows instead
//     re-executed every coroutine after each event until none progressed
//     (GW-16), going through the scheduler goroutine each time.
//
// A panic in a coroutine is recovered into a *PanicError with its stack
// (GW-10).
//
// A coroutine that does not yield for longer than the stuck timeout (real
// I/O in workflow code) makes Run return ErrStuck instead of crashing the
// process. The scheduler is unusable afterwards and must be closed. Stuck
// detection is done by one process-wide watchdog goroutine reading atomic
// counters, so hand-offs pay no timer operation.
package coro

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// DefaultStuckTimeout is how long a coroutine may run without yielding
// before Run gives up with ErrStuck.
const DefaultStuckTimeout = 10 * time.Second

var (
	// ErrStuck reports a coroutine that ran longer than the stuck timeout
	// without yielding. The scheduler is broken afterwards.
	ErrStuck = errors.New("coro: coroutine did not yield within the stuck timeout")
	// ErrClosed is returned by Run on a closed scheduler.
	ErrClosed = errors.New("coro: scheduler closed")
)

// PanicError is a panic recovered from a coroutine.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// Unwrap returns the panic value when it is an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// Scheduler runs the coroutines of one workflow execution.
//
// Not safe for concurrent use. The owner (the executor) calls Spawn, Run,
// Close, and wake-ups through futures and channels, from one goroutine at a
// time and never while Run is in progress. Coroutines call them from inside
// a run. Exclusive access passes between goroutines through channel
// hand-offs, which give the happens-before edges.
type Scheduler struct {
	// Storage for the first coroutines and their queue, and the root
	// context, in the scheduler's own allocation: a workflow's executor
	// usually needs nothing more.
	rootStore rootCtx
	allBuf    [4]*Coroutine
	readyBuf  [4]*Coroutine

	ready   queue[*Coroutine]
	live    int          // coroutines spawned and not finished
	all     []*Coroutine // coroutines not finished, for Close
	current *Coroutine
	err     error // first coroutine error, returned by Run
	closing bool
	closed  bool

	// yield returns control to Run (or Close) when no coroutine is ready, a
	// coroutine failed, or one finished during Close. A coroutine sends on it
	// only after winning ctl (running -> idle). Buffered: the sender never
	// waits for the receiver.
	yield chan struct{}
	// abort wakes Run (or Close) after the watchdog won ctl (running ->
	// broken).
	abort chan struct{}

	// ctl says who holds control: idle (the owner), running (a coroutine),
	// or broken. A coroutine handing control back and the watchdog both
	// change it from running with compare-and-swap, so exactly one wins:
	//   - if the coroutine wins, Run gets a yield;
	//   - if the watchdog wins, Run gets an abort, and the coroutine, at its
	//     next hand-off, sees broken and reaps instead of yielding.
	ctl atomic.Uint32

	// Watchdog state: written by the run, read by the watchdog goroutine.
	stuck    time.Duration
	switches atomic.Uint64
	inRun    atomic.Bool
	// Watchdog-private: last seen switches and when they last changed.
	wdSeen  uint64
	wdSince time.Time

	// Reaping (broken scheduler): the coroutine holding control, at its next
	// hand-off, makes every other coroutine exit, one at a time.
	reaping bool
	reaper  *Coroutine
	reaped  chan struct{}

	root Context
}

const (
	ctlIdle uint32 = iota
	ctlRunning
	ctlBroken
)

// testHookBeforeYield, set only by tests, runs just before a coroutine tries
// to hand control back to Run: it widens the race with the watchdog.
var testHookBeforeYield atomic.Pointer[func()]

func (s *Scheduler) broken() bool { return s.ctl.Load() == ctlBroken }

// Coroutine is one cooperatively scheduled goroutine.
type Coroutine struct {
	s      *Scheduler
	resume chan struct{}
	fn     func(Context) error
	ctx    Context
	gen    uint32 // incremented at park and at resume (odd while parked); see waitList
	queued bool
	done   bool
	exit   atomic.Bool
}

// NewScheduler returns an empty scheduler. stuck <= 0 means
// DefaultStuckTimeout. Close must be called to release it.
func NewScheduler(stuck time.Duration) *Scheduler {
	if stuck <= 0 {
		stuck = DefaultStuckTimeout
	}
	s := &Scheduler{
		yield:  make(chan struct{}, 1),
		abort:  make(chan struct{}, 1),
		reaped: make(chan struct{}),
		stuck:  stuck,
	}
	s.rootStore.s = s
	s.root = &s.rootStore
	s.all, s.ready.s = s.allBuf[:0], s.readyBuf[:0]
	watch(s)
	return s
}

// Root returns the scheduler's background context.
func (s *Scheduler) Root() Context { return s.root }

// Live reports the number of coroutines that have not finished.
func (s *Scheduler) Live() int { return s.live }

// Err returns the first coroutine error.
func (s *Scheduler) Err() error { return s.err }

// Spawn creates a coroutine running fn(ctx) and queues it. ctx must belong
// to this scheduler. The goroutine starts parked: fn runs when the
// coroutine's turn comes in a Run. During Close the coroutine is created
// already told to exit (Close runs it to its end); on a broken scheduler
// nothing is created.
func (s *Scheduler) Spawn(ctx Context, fn func(Context) error) {
	if s.broken() {
		return
	}
	c := &Coroutine{s: s, resume: make(chan struct{}), fn: fn, ctx: ctx}
	if s.closing {
		c.exit.Store(true)
	}
	s.live++
	s.all = append(s.all, c)
	go c.main()
	s.enqueue(c)
}

func (c *Coroutine) main() {
	defer c.finish()
	<-c.resume
	if c.exit.Load() {
		return
	}
	if err := c.run(); err != nil && c.s.err == nil {
		c.s.err = err
	}
}

func (c *Coroutine) run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return c.fn(c.ctx)
}

// finish runs on the coroutine's goroutine when fn returned or the coroutine
// was told to exit, then passes control on.
func (c *Coroutine) finish() {
	s := c.s
	c.done = true
	if !s.broken() {
		s.live--
		s.forget(c)
	}
	s.switchFrom(c)
}

// handOff is what a coroutine does after switchFrom.
type handOff uint8

const (
	handWait handOff = iota // wait for a resume
	handSelf                // keep running: it was itself the next ready coroutine
	handExit                // exit: the scheduler is closing or broken
)

// park suspends the running coroutine until it is woken and resumed. The
// caller re-checks its condition afterwards (wake-ups can be spurious). A
// coroutine told to exit (Close, a broken scheduler) never parks: it exits,
// running its deferred functions; one that blocks again exits again.
func (c *Coroutine) park() {
	if c.exit.Load() {
		runtime.Goexit()
	}
	c.gen++
	switch c.s.switchFrom(c) {
	case handSelf:
		c.gen++
		return
	case handExit:
		runtime.Goexit()
	case handWait:
	}
	<-c.resume
	c.gen++
	if c.exit.Load() {
		runtime.Goexit() // runs finish, which hands control back
	}
}

// switchFrom hands control from the coroutine from (parking or finished) to
// the next ready coroutine, or back to Run when there is none, a coroutine
// failed, or the scheduler is closing.
func (s *Scheduler) switchFrom(from *Coroutine) handOff {
	if s.broken() {
		return s.reap(from)
	}
	if s.err == nil && !s.closing {
		for s.ready.len() > 0 {
			next := s.ready.pop()
			next.queued = false
			if next.done {
				continue
			}
			s.switches.Add(1)
			if next == from {
				// Defensive: a running coroutine's registrations are not
				// valid until it parks, so it cannot be queued. Resuming
				// itself through its channel would deadlock.
				return handSelf
			}
			s.current = next
			next.resume <- struct{}{}
			return handWait
		}
	}
	s.current = nil
	if hook := testHookBeforeYield.Load(); hook != nil {
		(*hook)()
	}
	if !s.ctl.CompareAndSwap(ctlRunning, ctlIdle) {
		return s.reap(from) // the watchdog won: Run already returned ErrStuck
	}
	s.yield <- struct{}{}
	return handWait
}

// reap runs on the goroutine holding control of a broken scheduler, which
// was stuck and has now yielded or finished. It makes every other coroutine
// exit, one at a time, so their deferred functions never run in parallel,
// then exits itself. Coroutines exiting this way report back on reaped.
func (s *Scheduler) reap(from *Coroutine) handOff {
	if s.reaping {
		if from != s.reaper {
			s.reaped <- struct{}{}
		}
		return handExit
	}
	s.reaping, s.reaper = true, from
	from.exit.Store(true)
	for {
		var next *Coroutine
		for _, c := range s.all {
			if !c.done && c != from {
				next = c
				break
			}
		}
		if next == nil {
			break
		}
		next.exit.Store(true)
		next.resume <- struct{}{}
		<-s.reaped
	}
	return handExit
}

func (s *Scheduler) enqueue(c *Coroutine) {
	if c.queued || c.done {
		return
	}
	c.queued = true
	s.ready.push(c)
}

// Run resumes ready coroutines until none is ready. It returns the first
// coroutine error (the scheduler stays usable for Close), ErrStuck, or
// ErrClosed. After ErrStuck only Close may be called: the stuck coroutine
// may still be running and owns the scheduler's state.
func (s *Scheduler) Run() error {
	switch {
	case s.closed:
		return ErrClosed
	case s.broken():
		return ErrStuck
	case s.err != nil:
		return s.err
	}
	var first *Coroutine
	for s.ready.len() > 0 && first == nil {
		c := s.ready.pop()
		c.queued = false
		if !c.done {
			first = c
		}
	}
	if first == nil {
		return nil
	}
	if !s.handTo(first) {
		return ErrStuck
	}
	return s.err
}

// handTo gives control to c and waits until it comes back. It reports false
// when the watchdog broke the scheduler: the coroutine holding control then
// owns the state and reaps.
func (s *Scheduler) handTo(c *Coroutine) bool {
	s.current = c
	s.switches.Add(1)
	s.ctl.Store(ctlRunning)
	s.inRun.Store(true)
	c.resume <- struct{}{}
	select {
	case <-s.yield:
		s.inRun.Store(false)
		return true
	case <-s.abort:
		return false
	}
}

func (s *Scheduler) forget(c *Coroutine) {
	for i, x := range s.all {
		if x == c {
			last := len(s.all) - 1
			s.all[i] = s.all[last]
			s.all[last] = nil
			s.all = s.all[:last]
			return
		}
	}
}

// Close stops every unfinished coroutine, one at a time; their deferred
// functions run, and coroutines they spawn are stopped too. It releases the
// scheduler and is safe to call more than once.
//
// The watchdog keeps guarding while deferred functions run. If one gets
// stuck, or the scheduler was already broken, Close returns at once and the
// coroutine holding control stops the others when it next hands off.
func (s *Scheduler) Close() {
	if s.closed {
		return
	}
	s.closed = true
	defer unwatch(s)
	if s.broken() {
		return // the coroutine holding control reaps (see reap)
	}
	s.closing = true
	s.ready = queue[*Coroutine]{}
	for len(s.all) > 0 {
		c := s.all[0]
		if c.done { // cannot happen: finish forgets; defensive
			s.forget(c)
			continue
		}
		c.exit.Store(true)
		if !s.handTo(c) {
			return
		}
	}
	s.current = nil
	s.live = 0
}

// running returns the coroutine currently executing on ctx's scheduler and
// panics when called outside one (a programming error in the runtime's
// callers, recovered at the executor boundary).
func running(ctx Context) *Coroutine {
	c := ctx.scheduler().current
	if c == nil {
		panic("coro: blocking operation outside a coroutine")
	}
	return c
}
