package otelflows_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
	"github.com/imdat99/go-flows/otelflows"
)

var (
	traceStep = flows.DefineActivity("otel.trace-step", func(_ context.Context, _ flows.Key, s string) (string, struct{}, error) {
		return s, struct{}{}, nil
	}, nil)
	traceKid    = flows.DefineWorkflow("otel.trace-kid", 1, func(c flows.Ctx, s string) (string, error) { return traceStep.Run(c, s) })
	traceParent = flows.DefineWorkflow("otel.trace-parent", 1, func(c flows.Ctx, id string) (string, error) {
		if _, err := traceStep.Run(c, "p"); err != nil {
			return "", err
		}
		return traceKid.Child(c, id+"-kid", "k").Get(c)
	})
	traceTaken = flows.DefineWorkflow("otel.trace-taken", 1, func(c flows.Ctx, id string) (string, error) {
		_, err := traceKid.Child(c, id, "t").Get(c)
		if errors.Is(err, flows.ErrInstanceExists) {
			return "taken", nil
		}
		return "", err
	})
	traceLoop = flows.DefineWorkflow("otel.trace-loop", 1, func(c flows.Ctx, n int) (int, error) {
		if _, err := traceStep.Run(c, "l"); err != nil {
			return 0, err
		}
		if n < 1 {
			return 0, flows.ContinueAsNew(c, n+1)
		}
		return n, nil
	})
)

func attr(s sdktrace.ReadOnlySpan, k attribute.Key) (attribute.Value, bool) {
	for _, a := range s.Attributes() {
		if a.Key == k {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

// spansOf returns the ended spans of instance id, by name prefix.
func spansOf(rec *tracetest.SpanRecorder, id, prefix string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if v, _ := attr(s, otelflows.AttrInstanceID); v.AsString() == id && strings.HasPrefix(s.Name(), prefix) {
			out = append(out, s)
		}
	}
	return out
}

// run starts wf under ctx on a tester traced by tp and waits for it.
func run(ctx context.Context, t *testing.T, tp trace.TracerProvider, id string) *flowtest.Tester {
	t.Helper()
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Tracer: otelflows.NewTracer(tp)}))
	h, err := traceParent.Start(ctx, tst.Engine(), id, id)
	if err != nil {
		t.Fatal(err)
	}
	tst.Settle()
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := h.Wait(wctx); err != nil || out != "k" {
		t.Fatalf("%q %v", out, err)
	}
	return tst
}

func one(t *testing.T, spans []sdktrace.ReadOnlySpan, what string) sdktrace.ReadOnlySpan {
	t.Helper()
	if len(spans) != 1 {
		t.Fatalf("%d %s spans", len(spans), what)
	}
	return spans[0]
}

// T10.10: a workflow started inside a traced request is in the request's
// trace: its start span is a child of the caller's span, its tasks and
// attempts children of the start span; a child workflow's start span is a
// child of the parent's task that started it, and the child's tasks and
// attempts children of that.
func TestStartedInCallersTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, caller := tp.Tracer("app").Start(context.Background(), "http.request")
	run(ctx, t, tp, "req")
	caller.End()

	start := one(t, spansOf(rec, "req", "flows.start"), "start")
	if start.Parent().SpanID() != caller.SpanContext().SpanID() || start.SpanContext().TraceID() != caller.SpanContext().TraceID() {
		t.Fatal("the start span is not a child of the caller's")
	}
	tasks := spansOf(rec, "req", "flows.workflow_task")
	acts := spansOf(rec, "req", "flows.activity")
	if len(tasks) == 0 || len(acts) != 1 {
		t.Fatalf("%d tasks, %d attempts", len(tasks), len(acts))
	}
	for _, s := range append(tasks, acts...) {
		if s.Parent().SpanID() != start.SpanContext().SpanID() || s.SpanContext().TraceID() != start.SpanContext().TraceID() {
			t.Fatalf("%q is not a child of the start span", s.Name())
		}
		if v, _ := attr(s, otelflows.AttrExecutionID); v.AsInt64() == 0 {
			t.Fatalf("%q without an execution id", s.Name())
		}
	}
	// The child.
	kidStart := one(t, spansOf(rec, "req-kid", "flows.start"), "child start")
	var parentTask bool
	for _, s := range tasks {
		parentTask = parentTask || kidStart.Parent().SpanID() == s.SpanContext().SpanID()
	}
	if !parentTask || kidStart.SpanContext().TraceID() != caller.SpanContext().TraceID() {
		t.Fatal("the child's start span is not a child of a task of its parent")
	}
	if v, _ := attr(kidStart, otelflows.AttrParent); v.AsString() != "req" {
		t.Fatalf("child start parent attribute %q", v.AsString())
	}
	kidSpans := append(spansOf(rec, "req-kid", "flows.workflow_task"), spansOf(rec, "req-kid", "flows.activity")...)
	if len(kidSpans) < 2 {
		t.Fatalf("%d child spans", len(kidSpans))
	}
	for _, s := range kidSpans {
		if s.Parent().SpanID() != kidStart.SpanContext().SpanID() {
			t.Fatalf("child %q is not a child of the child's start span", s.Name())
		}
	}
}

// T10.10: the sampling decision taken at the start holds for the whole
// execution, in every later task and attempt: a ratio of 0 records nothing.
func TestSampledOutAtStart(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0))))
	run(context.Background(), t, tp, "unsampled")
	if n := len(rec.Ended()); n != 0 {
		t.Fatalf("%d spans recorded", n)
	}
}

// T10.10: executions of two stores (two environments) with the same
// instance id are two traces: trace ids are not derived from ids.
func TestSameInstanceIDInTwoStores(t *testing.T) {
	var traces []trace.TraceID
	for range 2 {
		rec := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		run(context.Background(), t, tp, "same")
		start := one(t, spansOf(rec, "same", "flows.start"), "start")
		if start.Parent().IsValid() {
			t.Fatal("a start without a caller's span has a parent")
		}
		traces = append(traces, start.SpanContext().TraceID())
	}
	if traces[0] == traces[1] {
		t.Fatal("two stores share a trace")
	}
}

// T10.10: an attempt whose lease was lost ends with an error status and
// outcome lease_lost; the attempt that replaces it, possibly in another
// process (another provider), is in the same trace under the same start
// span.
func TestLostLeaseAttemptInSameTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tr := otelflows.NewTracer(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	ctx := context.Background()
	stored, end := tr.StartWorkflow(ctx, flows.StartInfo{InstanceID: "i", Workflow: "wf", Version: 1})
	end(true, nil)
	if len(stored) == 0 || len(stored) > flows.MaxTraceContext {
		t.Fatalf("trace context of %d bytes", len(stored))
	}
	_, end1 := tr.StartActivity(ctx, flows.ActivityInfo{InstanceID: "i", ExecutionID: 7, Activity: "a", Attempt: 1, Trace: stored})
	end1(fmt.Errorf("record: %w", flows.ErrLeaseLost))
	other := tracetest.NewSpanRecorder()
	tr2 := otelflows.NewTracer(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(other)))
	_, end2 := tr2.StartActivity(ctx, flows.ActivityInfo{InstanceID: "i", ExecutionID: 7, Activity: "a", Attempt: 2, Uncertain: true, Trace: stored})
	end2(nil)

	start := one(t, spansOf(rec, "i", "flows.start"), "start")
	lost := one(t, spansOf(rec, "i", "flows.activity"), "lost attempt")
	next := one(t, spansOf(other, "i", "flows.activity"), "next attempt")
	if lost.Status().Code != codes.Error {
		t.Fatalf("lost attempt status %v", lost.Status())
	}
	if v, _ := attr(lost, otelflows.AttrOutcome); v.AsString() != "lease_lost" {
		t.Fatalf("lost attempt outcome %q", v.AsString())
	}
	if v, _ := attr(lost, otelflows.AttrAttempt); v.AsInt64() != 1 {
		t.Fatalf("lost attempt %v", v)
	}
	for _, s := range []sdktrace.ReadOnlySpan{lost, next} {
		if s.SpanContext().TraceID() != start.SpanContext().TraceID() || s.Parent().SpanID() != start.SpanContext().SpanID() {
			t.Fatalf("attempt %v not under the start span", attr2(s))
		}
	}
	if next.Status().Code == codes.Error {
		t.Fatalf("next attempt status %v", next.Status())
	}
	if v, _ := attr(next, otelflows.AttrOutcome); v.AsString() == "lease_lost" {
		t.Fatal("next attempt marked lost")
	}
}

func attr2(s sdktrace.ReadOnlySpan) int64 {
	v, _ := attr(s, otelflows.AttrAttempt)
	return v.AsInt64()
}

// T10.10 review: a start that creates nothing is marked: a second Start
// (flows.outcome existing, not an error) and a child whose instance id is
// taken (existing, error status).
func TestStartsThatCreateNothing(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tst := run(context.Background(), t, tp, "dup")
	if _, err := traceParent.Start(context.Background(), tst.Engine(), "dup", "dup"); err != nil {
		t.Fatal(err)
	}
	if out, err := flowtest.Run(tst, traceTaken, "taker", "dup"); err != nil || out != "taken" {
		t.Fatalf("%q %v", out, err)
	}
	starts := spansOf(rec, "dup", "flows.start")
	if len(starts) != 3 {
		t.Fatalf("%d start spans of dup", len(starts))
	}
	for i, s := range starts {
		outcome, _ := attr(s, otelflows.AttrOutcome)
		_, child := attr(s, otelflows.AttrParent)
		switch {
		case i == 0:
			if outcome.AsString() != "" || s.Status().Code == codes.Error {
				t.Fatalf("first start: outcome %q status %v", outcome.AsString(), s.Status())
			}
		case child: // the taken child
			if outcome.AsString() != "existing" || s.Status().Code != codes.Error {
				t.Fatalf("taken child start: outcome %q status %v", outcome.AsString(), s.Status())
			}
		default: // the second Start
			if outcome.AsString() != "existing" || s.Status().Code == codes.Error {
				t.Fatalf("second start: outcome %q status %v", outcome.AsString(), s.Status())
			}
		}
	}
}

// T10.10 review: continue-as-new starts a new trace, linked to the start
// span of the execution it continues; each execution's spans are in its
// own trace.
func TestContinueAsNewStartsALinkedTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	ctx, caller := tp.Tracer("app").Start(context.Background(), "http.request")
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Tracer: otelflows.NewTracer(tp)}))
	h, err := traceLoop.Start(ctx, tst.Engine(), "loop", 0)
	if err != nil {
		t.Fatal(err)
	}
	tst.Settle()
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := h.Wait(wctx); err != nil || out != 1 {
		t.Fatalf("%d %v", out, err)
	}
	caller.End()
	starts := spansOf(rec, "loop", "flows.start")
	if len(starts) != 2 {
		t.Fatalf("%d start spans", len(starts))
	}
	first, next := starts[0], starts[1]
	if first.SpanContext().TraceID() != caller.SpanContext().TraceID() {
		t.Fatal("the first execution is not in the caller's trace")
	}
	if next.Parent().IsValid() || next.SpanContext().TraceID() == first.SpanContext().TraceID() {
		t.Fatal("continue-as-new did not start a new trace")
	}
	if v, _ := attr(next, otelflows.AttrContinued); !v.AsBool() {
		t.Fatal("continued start not marked")
	}
	if l := next.Links(); len(l) != 1 || l[0].SpanContext.TraceID() != first.SpanContext().TraceID() || l[0].SpanContext.SpanID() != first.SpanContext().SpanID() {
		t.Fatalf("links %+v", l)
	}
	// Each execution's tasks and attempts are under its own start span.
	for _, s := range append(spansOf(rec, "loop", "flows.workflow_task"), spansOf(rec, "loop", "flows.activity")...) {
		p := s.Parent().SpanID()
		if p != first.SpanContext().SpanID() && p != next.SpanContext().SpanID() {
			t.Fatalf("%q under neither start span", s.Name())
		}
	}
}
