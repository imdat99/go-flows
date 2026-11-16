package flows

import (
	"testing"

	"github.com/imdat99/go-flows/internal/store"
)

func TestActivityKeyFormat(t *testing.T) {
	task := store.ActivityTask{InstanceID: "order:7", ExecutionID: 1234567, ScheduleID: 12, Name: "pay.charge"}
	if got, want := activityKey(&task), Key("order:7:12:pay.charge:1234567"); got != want {
		t.Fatalf("key %q, want %q", got, want)
	}
	task.LegacyKey = true
	if got, want := activityKey(&task), Key("order:7:12:pay.charge"); got != want {
		t.Fatalf("legacy key %q, want %q", got, want)
	}
	if n := testing.AllocsPerRun(100, func() { _ = activityKey(&task) }); n > 1 {
		t.Fatalf("%v allocations per key", n)
	}
	task.LegacyKey = false
	if n := testing.AllocsPerRun(100, func() { _ = activityKey(&task) }); n > 1 {
		t.Fatalf("%v allocations per key", n)
	}
}
