package mem

import (
	"context"
	"testing"

	"github.com/imdat99/go-flows/internal/store"
)

// T10.14: without an end hook, an end costs the hook nothing.
func TestNoHookAllocatesNothing(t *testing.T) {
	s := New(nil)
	e := &execution{}
	e.ID, e.InstanceID, e.Workflow, e.Version = 1, "i", "wf", 1
	ctx := context.Background()
	if n := testing.AllocsPerRun(100, func() {
		if err := s.callHook(ctx, e, store.StatusCompleted, "", ""); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Errorf("end without a hook: %v allocations", n)
	}
}
