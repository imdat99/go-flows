// Package adapt holds what the process packages share to adapt the domain
// modules to the engine: the holder of a process's module services, and the
// translation of a module's errors into the engine's retry decision.
//
// # Why a holder
//
// go-flows definitions (DefineActivity, DefineWorkflow) are package-level
// values in a global registry, as database/sql drivers are: an activity
// function cannot take constructor arguments. Each process package keeps
// the module services its activities call in one Ref, set once by its
// Configure from the composition root before the engine starts. This is
// the only package-level mutable state of the example; the modules have
// none, and take their dependencies in constructors.
package adapt

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/imdat99/go-flows/flows"
)

// ErrNotConfigured is the error of a process used before its Configure.
//
// It is not permanent: an activity failing with it is retried, so a replica
// started with a wiring mistake delays work instead of failing (and
// compensating) real orders. The composition root configures every process
// at startup and refuses to start on an error.
var ErrNotConfigured = errors.New("process not configured")

// Ref holds the services of one process.
type Ref[T any] struct {
	process string
	p       atomic.Pointer[T]
}

// NewRef returns an empty Ref for process.
func NewRef[T any](process string) *Ref[T] { return &Ref[T]{process: process} }

// Set stores v.
func (r *Ref[T]) Set(v T) { r.p.Store(&v) }

// Get returns the stored services, or ErrNotConfigured.
func (r *Ref[T]) Get() (*T, error) {
	if v := r.p.Load(); v != nil {
		return v, nil
	}
	return nil, fmt.Errorf("%s: %w (call Configure before starting the engine)", r.process, ErrNotConfigured)
}

// Error translates a module's error for the engine. A business rejection
// (isRejection, the module's own classification) becomes permanent: no
// retry, the calling saga compensates. Anything else is returned as is,
// and retried by the activity's policy.
func Error(err error, isRejection func(error) bool) error {
	if err != nil && isRejection(err) {
		return flows.Permanent(err)
	}
	return err
}
