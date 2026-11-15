package pg

import (
	"strconv"
	"testing"

	"github.com/imdat99/go-flows/internal/history"
)

// The SQL statements embed event types as literals: they must match.
func TestTypeLiterals(t *testing.T) {
	for lit, typ := range map[string]history.Type{
		typeStarted: history.ExecutionStarted, typeTerminated: history.ExecutionTerminated,
		typeCancelRequested: history.ExecutionCancelRequested, typeTimerFired: history.TimerFired,
		typeSignal: history.SignalReceived, typeChildFailed: history.ChildFailed,
		typeRetryCompensation: history.CompensationRetryRequested,
	} {
		if lit != strconv.Itoa(int(typ)) {
			t.Errorf("literal %s for %v (%d)", lit, typ, typ)
		}
	}
}
