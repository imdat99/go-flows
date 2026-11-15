package otelflows_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	flows "github.com/imdat99/go-flows"
	"github.com/imdat99/go-flows/flowtest"
	"github.com/imdat99/go-flows/otelflows"
)

var (
	errDeclined = errors.New("declined")
	inner       = func() bool { flows.RegisterErrorKind("otel_declined", errDeclined); return true }()

	reserve = flows.DefineActivity("otel.reserve", func(ctx context.Context, _ flows.Key, sku string) (string, string, error) {
		// A span of the activity's own: a child of the attempt's.
		_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer("app").Start(ctx, "db.insert")
		span.End()
		return sku, sku, nil
	}, func(context.Context, flows.Key, string) error { return nil })
	pay = flows.DefineActivity("otel.pay", func(context.Context, flows.Key, int) (int, struct{}, error) {
		return 0, struct{}{}, flows.Permanent(errDeclined)
	}, nil)
	order = flows.DefineWorkflow("otel.order", 1, func(c flows.Ctx, _ int) (int, error) {
		if _, err := reserve.Run(c, "book"); err != nil {
			return 0, err
		}
		return pay.Run(c, 10)
	})
)

func TestTracesAndMetrics(t *testing.T) {
	_ = inner
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	obs, err := otelflows.NewObserver(mp)
	if err != nil {
		t.Fatal(err)
	}
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Tracer: otelflows.NewTracer(tp), Observer: obs}))
	if _, err := flowtest.Run(tst, order, "o1", 0); !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("%v", err)
	}
	// Spans.
	byName := map[string]sdktrace.ReadOnlySpan{}
	var names []string
	for _, s := range spans.Ended() {
		byName[s.Name()] = s
		names = append(names, s.Name())
	}
	for _, want := range []string{"flows.activity otel.reserve", "flows.activity otel.pay", "flows.compensate otel.reserve", "db.insert", "flows.workflow_task otel.order"} {
		if !slices.Contains(names, want) {
			t.Fatalf("no span %q in %v", want, names)
		}
	}
	if db, act := byName["db.insert"], byName["flows.activity otel.reserve"]; db.Parent().SpanID() != act.SpanContext().SpanID() {
		t.Fatal("the activity's span is not a child of the attempt's")
	}
	if s := byName["flows.activity otel.pay"]; s.Status().Code != codes.Error {
		t.Fatalf("failed attempt status %v", s.Status())
	}
	statuses := map[string]bool{}
	for _, s := range spans.Ended() {
		for _, a := range s.Attributes() {
			if a.Key == otelflows.AttrStatus {
				statuses[a.Value.AsString()] = true
			}
		}
	}
	if !statuses["compensated"] {
		t.Fatalf("no workflow task ended compensated: %v", statuses)
	}
	// Metrics.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if s, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					sums[m.Name+"|"+label(dp.Attributes)] += dp.Value
				}
			}
		}
	}
	for key, want := range map[string]int64{
		"flows.activity.attempts|otel.reserve/completed/false": 1,
		"flows.activity.attempts|otel.reserve/completed/true":  1,
		"flows.activity.attempts|otel.pay/failed/false":        1,
		"flows.runs.ended|otel.order/compensated":              1,
	} {
		if sums[key] != want {
			t.Fatalf("%s = %d, want %d (all: %v)", key, sums[key], want, sums)
		}
	}
}

func label(set attribute.Set) string {
	get := func(k attribute.Key) string {
		v, ok := set.Value(k)
		if !ok {
			return ""
		}
		return v.String()
	}
	if w := get(otelflows.AttrWorkflow); w != "" {
		return w + "/" + get(otelflows.AttrStatus)
	}
	return get(otelflows.AttrActivity) + "/" + get(otelflows.AttrOutcome) + "/" + get(otelflows.AttrCompensation)
}
