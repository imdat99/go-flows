package storetest

import (
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
)

// realTime runs a case whose short TTL must both last (between some steps)
// and lapse (where the case advances the clock past it). On Postgres the
// clock is real: a step slower than the TTL (a loaded machine) makes the
// TTL lapse too early, and the run proves nothing. run reports that with
// false (checked with within before each assertion that relies on the TTL
// lasting), and the case runs again on a new store with h.TTL doubled.
// The memory store's clock moves only when advanced: one run.
func realTime(t *testing.T, h Harness, run func(h Harness) bool) {
	t.Helper()
	base := h.TTL
	for {
		if run(h) {
			return
		}
		if h.TTL >= 8*base {
			t.Fatalf("steps outlasted a TTL of %v (%v doubled 3 times)", h.TTL, base)
		}
		t.Logf("a step outlasted the TTL of %v; again with %v", h.TTL, 2*h.TTL)
		h.TTL *= 2
	}
}

// within returns a check that less than d of the store's time has passed
// since within was called: anything the store did after the call with a
// TTL of d (a lock, a turn, a release delay) has not lapsed yet when the
// check holds.
func within(t *testing.T, s store.Store, d time.Duration) func() bool {
	t.Helper()
	start := storeNow(t, s)
	return func() bool { return storeNow(t, s).Sub(start) < d }
}

// lease is the lease of the tasks the lock waiter tests claim: long enough
// not to lapse while a task runs on a loaded machine (a lapse is not what
// they test: the task would be claimed again).
func lease(h Harness) time.Duration { return waitTTL(h) }
