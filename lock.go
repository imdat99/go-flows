package flows

import (
	"fmt"
	"time"

	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/text"
)

// Ported in spirit from saga (lock.go, same author): business locks tied to
// the lease of the execution that holds them.

var (
	// ErrLockHeld is the error of Lock when another execution holds the
	// lock (after LockWait's policy ran out).
	ErrLockHeld = executor.ErrLockHeld
	// ErrLockLost ends a workflow whose lock lapsed while it was not running
	// (sleeping, awaiting, parked, or its process died) and was taken by
	// another execution: what it protected was not exclusive. The workflow
	// fails with it and compensates.
	ErrLockLost = executor.ErrLockLost
)

// minLockWait floors LockWait's delays: a zero delay would loop. The
// floored delay is recorded (a timer): changing it parks executions waiting
// in LockWait as non-deterministic when they replay.
const minLockWait = 10 * time.Millisecond

// LockOption configures Lock.
type LockOption func(*lockConfig)

type lockConfig struct{ wait RetryPolicy }

// LockWait makes Lock wait for a busy lock instead of failing: the workflow
// sleeps (a durable timer, at least 10ms) and asks again after each delay
// of p, until p is exhausted. Each attempt adds a few events to the
// history: for very long waits, prefer a policy with growing delays.
func LockWait(p RetryPolicy) LockOption { return func(c *lockConfig) { c.wait = p } }

// Lock takes the business lock key for the rest of the execution: it is
// released when the execution ends (completed, failed, compensated,
// canceled, terminated, or continued as new). Taking a lock the execution
// already holds succeeds.
//
// While the execution runs (a task) or one of its activities runs
// (compensators included), lease renewals keep the lock alive. While
// neither does (it sleeps, awaits a signal, waits in LockWait for another
// lock, is parked, or its processes died), the lock lapses after ttl (at
// least the engine's LeaseTTL), and another execution may take it: the
// workflow then fails with ErrLockLost at its next task. So ttl bounds how
// long a waiting execution keeps others out; an activity waiting in a queue
// or between retries does not run, so choose ttl above the expected queue
// wait and retry delays. A compensating execution that
// loses a lock still completes its compensation.
//
// Locks are polled, not queued: waiters are not served in order. Two
// executions taking two locks in opposite orders with LockWait each lose
// the first lock once they wait longer than its ttl. Take locks in a fixed
// order.
//
// If another execution holds the lock, Lock fails with ErrLockHeld (the
// holder's instance id in the message), or with LockWait retries.
func Lock(c Ctx, key string, ttl time.Duration, opts ...LockOption) error {
	st := c.state()
	if err := text.CheckName(ErrInvalidArgument, "lock key", key); err != nil {
		return Permanent(err)
	}
	if ttl <= 0 {
		return Permanent(fmt.Errorf("%w: Lock(%q) with ttl %v", ErrInvalidArgument, key, ttl))
	}
	var cfg lockConfig
	for _, o := range opts {
		o(&cfg)
	}
	for attempt := int32(1); ; attempt++ {
		err := st.Lock(c.c, key, ttl)
		if err == nil || attempt >= int32(cfg.wait.Attempts) {
			return err
		}
		if err := Sleep(c, max(cfg.wait.Delay(attempt+1), minLockWait)); err != nil {
			return err
		}
	}
}
