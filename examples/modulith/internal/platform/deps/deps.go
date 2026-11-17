// Package deps holds a module's dependencies for its activities.
//
// Activities are package-level values (flows.DefineActivity registers them
// in a global registry, as database/sql drivers are), so they cannot take
// constructor arguments. Each module keeps its dependencies in a Ref, set
// once by the composition root (the module's Configure) before the engine
// starts, and read by its activities.
package deps

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrNotConfigured is the error of a module used before its Configure.
//
// It is not permanent: an activity failing with it is retried, so a replica
// started with a wiring mistake delays work instead of failing (and
// compensating) real orders. The composition root checks every module's
// configuration at startup and refuses to start without it.
var ErrNotConfigured = errors.New("module not configured")

// Ref holds the dependencies of one module.
type Ref[T any] struct {
	module string
	p      atomic.Pointer[T]
}

// New returns an empty Ref for module.
func New[T any](module string) *Ref[T] { return &Ref[T]{module: module} }

// Set stores v.
func (r *Ref[T]) Set(v T) { r.p.Store(&v) }

// Get returns the stored dependencies, or ErrNotConfigured.
func (r *Ref[T]) Get() (*T, error) {
	if v := r.p.Load(); v != nil {
		return v, nil
	}
	return nil, fmt.Errorf("%s: %w (call Configure before starting the engine)", r.module, ErrNotConfigured)
}
