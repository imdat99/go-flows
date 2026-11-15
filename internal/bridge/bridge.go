// Package bridge lets package flowtest build engines with the internals of
// package flows, without exporting them: flows sets these functions in its
// init, flowtest calls them.
package bridge

import (
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/worker"
)

// Mocks returns the mocked activity (or compensator) called name, if any.
type Mocks func(name string, compensate bool) (*worker.Activity, bool)

var (
	// NewEngine returns a stopped *flows.Engine on st (opts is a
	// flows.Options), whose activities are looked up in mocks first, and
	// its worker.
	NewEngine func(st store.Store, mocks Mocks, opts any) (engine any, w *worker.Worker, err error)
	// Mock builds what runs fn in place of act (a *flows.Activity[I, O,
	// C]): its invocation (fn a flows.InvokeFunc[I, O, C]), or its
	// compensator (fn a flows.CompensateFunc[C]); and act's name.
	Mock func(act, fn any, compensate bool) (name string, a *worker.Activity, err error)
)
