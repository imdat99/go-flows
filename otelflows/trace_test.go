package otelflows_test

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/otelflows"
)

func attr(s sdktrace.ReadOnlySpan, k attribute.Key) (attribute.Value, bool) {
	for _, a := range s.Attributes() {
		if a.Key == k {
			return a.Value, true
		}
	}
	return attribute.Value{}, false
}

// T10.10: the tasks and attempts of one execution share a trace (derived
// from the execution, nothing stored), children of one execution span
// context; an attempt whose lease was lost ends with an error status and
// outcome lease_lost, and the next attempt joins the same trace.
func TestExecutionTrace(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	tr := otelflows.NewTracer(tp)
	ctx := context.Background()
	lost := fmt.Errorf("record: %w", flows.ErrLeaseLost)

	endTask := tr.StartWorkflowTask(ctx, flows.WorkflowTaskInfo{InstanceID: "i", ExecutionID: 42, Workflow: "wf", Version: 1, Status: flows.StatusRunning})
	endTask("", nil)
	_, end1 := tr.StartActivity(ctx, flows.ActivityInfo{InstanceID: "i", ExecutionID: 42, Activity: "a", Attempt: 1})
	end1(lost)
	_, end2 := tr.StartActivity(ctx, flows.ActivityInfo{InstanceID: "i", ExecutionID: 42, Activity: "a", Attempt: 2, Uncertain: true})
	end2(nil)
	endLostTask := tr.StartWorkflowTask(ctx, flows.WorkflowTaskInfo{InstanceID: "i", ExecutionID: 42, Workflow: "wf", Version: 1, Status: flows.StatusRunning})
	endLostTask("", lost)
	// Another execution of the same instance id (continue-as-new).
	endOther := tr.StartWorkflowTask(ctx, flows.WorkflowTaskInfo{InstanceID: "i", ExecutionID: 43, Workflow: "wf", Version: 1, Status: flows.StatusRunning})
	endOther(flows.StatusCompleted, nil)

	ended := spans.Ended()
	if len(ended) != 5 {
		t.Fatalf("%d spans", len(ended))
	}
	first := ended[0]
	if !first.Parent().IsValid() || !first.Parent().IsRemote() {
		t.Fatalf("task span parent %+v: want the execution's (remote) span context", first.Parent())
	}
	for _, s := range ended[:4] {
		if s.SpanContext().TraceID() != first.SpanContext().TraceID() || s.Parent().SpanID() != first.Parent().SpanID() {
			t.Fatalf("span %q not in the execution's trace", s.Name())
		}
		if v, ok := attr(s, otelflows.AttrExecutionID); !ok || v.AsInt64() != 42 {
			t.Fatalf("span %q execution id %v %v", s.Name(), v, ok)
		}
	}
	if ended[4].SpanContext().TraceID() == first.SpanContext().TraceID() {
		t.Fatal("two executions share a trace")
	}
	for _, s := range []sdktrace.ReadOnlySpan{ended[1], ended[3]} {
		if s.Status().Code != codes.Error {
			t.Fatalf("lost %q: status %v", s.Name(), s.Status())
		}
		if v, _ := attr(s, otelflows.AttrOutcome); v.AsString() != "lease_lost" {
			t.Fatalf("lost %q: outcome %q", s.Name(), v.AsString())
		}
	}
	if v, _ := attr(ended[1], otelflows.AttrAttempt); v.AsInt64() != 1 {
		t.Fatalf("lost attempt %v", v)
	}
	if s := ended[2]; s.Status().Code == codes.Error {
		t.Fatalf("next attempt status %v", s.Status())
	}
	if v, _ := attr(ended[2], otelflows.AttrOutcome); v.AsString() == "lease_lost" {
		t.Fatal("next attempt marked lost")
	}
	// The same execution traced by another process (another provider)
	// gets the same trace.
	again := tracetest.NewSpanRecorder()
	otherTr := otelflows.NewTracer(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(again)))
	otherTr.StartWorkflowTask(ctx, flows.WorkflowTaskInfo{InstanceID: "i", ExecutionID: 42, Workflow: "wf", Version: 1})("", nil)
	if s := again.Ended()[0]; s.SpanContext().TraceID() != first.SpanContext().TraceID() || s.Parent().SpanID() != first.Parent().SpanID() {
		t.Fatal("another process traces the execution elsewhere")
	}
}
