package mem_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	var clocks sync.Map // store.Store -> *mem.ManualClock
	storetest.Run(t, storetest.Harness{
		New: func(*testing.T) store.Store {
			c := &mem.ManualClock{}
			s := mem.New(c)
			clocks.Store(s, c)
			return s
		},
		Advance: func(t *testing.T, s store.Store, d time.Duration) {
			t.Helper()
			c, ok := clocks.Load(s)
			if !ok {
				t.Fatal("unknown store")
			}
			c.(*mem.ManualClock).Advance(d)
		},
		SetHook: func(s store.Store, h func(context.Context, store.EndInfo) error) {
			s.(*mem.Store).SetHook(h)
		},
	})
}
