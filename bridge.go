package flows

import (
	"context"
	"errors"
	"fmt"

	"github.com/imdat99/go-flows/internal/bridge"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/worker"
)

// The flowtest bridge (internal/bridge).
func init() {
	bridge.NewEngine = func(st store.Store, mocks bridge.Mocks, opts any) (any, *worker.Worker, error) {
		o, ok := opts.(Options)
		if !ok {
			return nil, nil, errors.New("flows: bridge: options are not flows.Options")
		}
		e, err := newEngineWith(st, mockRegistry{mocks}, o)
		if err != nil {
			return nil, nil, err
		}
		return e, e.w, nil
	}
	bridge.Mock = func(act, fn any, compensate bool) (string, *worker.Activity, error) {
		m, ok := act.(mockable)
		if !ok {
			return "", nil, fmt.Errorf("flows: %T is not an activity", act)
		}
		var a *worker.Activity
		var err error
		if compensate {
			a, err = m.mockCompensator(fn)
		} else {
			a, err = m.mock(fn)
		}
		return m.Name(), a, err
	}
}

// mockable is implemented by every *Activity[I, O, C].
type mockable interface {
	Name() string
	mock(fn any) (*worker.Activity, error)
	mockCompensator(fn any) (*worker.Activity, error)
}

// mock runs fn (an InvokeFunc[I, O, C]) in place of the activity's
// invocation, with its options; its compensation data is stored when the
// activity has a compensator.
func (a *Activity[I, O, C]) mock(fn any) (*worker.Activity, error) {
	f, ok := fn.(func(context.Context, Key, I) (O, C, error))
	if !ok {
		var want func(context.Context, Key, I) (O, C, error)
		return nil, fmt.Errorf("flows: mock of %s must be a %T, not %T", a.name, want, fn)
	}
	_, hasComp := (workerRegistry{}).Activity(a.name, true)
	return a.invoker(f, hasComp), nil
}

// mockCompensator runs fn (a CompensateFunc[C]) in place of the
// activity's compensator.
func (a *Activity[I, O, C]) mockCompensator(fn any) (*worker.Activity, error) {
	f, ok := fn.(func(context.Context, Key, C) error)
	if !ok {
		var want func(context.Context, Key, C) error
		return nil, fmt.Errorf("flows: compensator mock of %s must be a %T, not %T", a.name, want, fn)
	}
	if _, has := (workerRegistry{}).Activity(a.name, true); !has {
		// Without a compensator no compensation data is stored: the mock
		// would never run.
		return nil, fmt.Errorf("flows: %s has no compensator to mock", a.name)
	}
	return a.compensator(f), nil
}

// mockRegistry looks activities up in the mocks first.
type mockRegistry struct{ mocks bridge.Mocks }

func (m mockRegistry) Activity(name string, compensate bool) (*worker.Activity, bool) {
	if m.mocks != nil {
		if a, ok := m.mocks(name, compensate); ok {
			return a, true
		}
	}
	return workerRegistry{}.Activity(name, compensate)
}
