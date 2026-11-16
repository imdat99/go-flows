package flowtest

import (
	"testing"
	"time"
)

// SetIdleTimeout shortens how long (wall time) waitIdle waits for an idle
// engine, until the test ends.
func SetIdleTimeout(tb testing.TB, d time.Duration) {
	tb.Helper()
	prev := idleTimeout
	idleTimeout = d
	tb.Cleanup(func() { idleTimeout = prev })
}
