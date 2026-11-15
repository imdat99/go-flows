package flows

import (
	"context"
	"strconv"
	"testing"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

type benchItem struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

// BenchmarkCodec: the typed codec on the data path (per activity input,
// output and compensation data).
func BenchmarkCodec(b *testing.B) {
	cd := newCodec[benchItem]()
	in := benchItem{SKU: "sku-123", Qty: 7}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := cd.encode(in); err != nil {
				b.Fatal(err)
			}
		}
	})
	raw, _ := cd.encode(in)
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := cd.decode(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("lookup", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = newCodec[benchItem]()
		}
	})
}

var (
	benchStep = DefineActivity("bench.step", func(_ context.Context, _ Key, in benchItem) (benchItem, struct{}, error) {
		in.Qty++
		return in, struct{}{}, nil
	}, nil)
	benchChain = DefineWorkflow("bench.chain", 1, func(c Ctx, in benchItem) (benchItem, error) {
		for range 10 {
			var err error
			if in, err = benchStep.Run(c, in); err != nil {
				return in, err
			}
		}
		return in, nil
	})
)

// BenchmarkPublicActivityRun: BenchmarkActivityRun (internal/worker) through
// the public typed API: one op is a workflow of 10 sequential activities on
// Postgres with eager execution, started with Start; its end is observed in
// the store (as the worker benchmark does) rather than with Wait, whose
// polling backoff would dominate.
func BenchmarkPublicActivityRun(b *testing.B) {
	pool := testdb.New(b)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		b.Fatal(err)
	}
	st := &endSignal{Store: pg.New(pool), ended: make(chan struct{}, 1)}
	e, err := newEngine(st, Options{InstanceID: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = e.Stop(ctx) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := benchChain.Start(ctx, e, "bench-"+strconv.Itoa(i), benchItem{SKU: "s"}); err != nil {
			b.Fatal(err)
		}
		<-st.ended
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*10), "µs/activity")
	out, err := benchChain.Handle(e, "bench-0").Wait(ctx)
	if err != nil || out.Qty != 10 {
		b.Fatalf("%v %v", out, err)
	}
}

type endSignal struct {
	*pg.Store
	ended chan struct{}
}

func (s *endSignal) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	leases, err := s.Store.CompleteWorkflowTask(ctx, l, c)
	if err == nil && c.Status == store.StatusCompleted {
		s.ended <- struct{}{}
	}
	return leases, err
}
