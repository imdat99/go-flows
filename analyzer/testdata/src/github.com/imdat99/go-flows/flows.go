// Package flows is a stub of github.com/imdat99/go-flows for the analyzer's
// tests.
package flows

import "context"

type Ctx struct{}

type WaitGroup struct{}

func Go(c Ctx, fn func(c Ctx)) {}

func SideEffect[T any](c Ctx, fn func() T) (T, error) {
	var zero T
	return zero, nil
}

type Workflow[I, O any] struct{}

func DefineWorkflow[I, O any](name string, version int, fn func(c Ctx, in I) (O, error)) *Workflow[I, O] {
	return nil
}

type Key string

func DefineActivity[I, O, C any](name string, invoke func(ctx context.Context, k Key, in I) (O, C, error)) {}
