// Package otelflows instruments go-flows engines with OpenTelemetry:
// traces (a span per start of an execution, per workflow task, and per
// activity attempt, the parent of the spans the activity makes) and metrics
// (activity attempts and their durations, executions ended).
//
//	tr := otelflows.NewTracer(nil)          // the global TracerProvider
//	obs, err := otelflows.NewObserver(nil)  // the global MeterProvider
//	engine, err := flows.New(pool, flows.Options{Tracer: tr, Observer: obs})
//
// It is a separate module, so go-flows itself does not depend on
// OpenTelemetry; an engine without a Tracer or Observer pays nothing.
//
// Traces follow executions. Workflow.Start emits a short span
// "flows.start <workflow>", a child of the span in the caller's context (a
// new root without one); its span context (trace id, span id, flags: 26
// bytes, the trace state is not kept) is stored with the execution and
// comes back with every claim, so every workflow task and activity attempt
// of the execution, in any process and however long after, is a child of
// that start span. A child workflow's start span is a child of the task
// that started it; continue-as-new keeps the trace. The sampling decision
// is the one taken at the start (the flags stored): a ParentBased sampler
// keeps or drops a whole execution with it.
//
// An attempt or task whose lease was lost (another engine runs it again)
// ends with an error status and flows.outcome "lease_lost"; the attempt
// that replaces it is in the same trace, under the same start span.
package otelflows

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	flows "github.com/imdat99/go-flows/flows"
)

const scope = "github.com/imdat99/go-flows/otelflows"

// Attribute keys.
const (
	AttrInstanceID   = attribute.Key("flows.instance_id")
	AttrExecutionID  = attribute.Key("flows.execution_id")
	AttrParent       = attribute.Key("flows.parent_instance_id")
	AttrWorkflow     = attribute.Key("flows.workflow")
	AttrVersion      = attribute.Key("flows.workflow_version")
	AttrActivity     = attribute.Key("flows.activity")
	AttrAttempt      = attribute.Key("flows.attempt")
	AttrCompensation = attribute.Key("flows.compensation")
	AttrUncertain    = attribute.Key("flows.uncertain")
	AttrStatus       = attribute.Key("flows.status")
	AttrOutcome      = attribute.Key("flows.outcome")
)

// NewTracer returns a flows.Tracer on tp (nil: the global provider).
func NewTracer(tp trace.TracerProvider) flows.Tracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tracer{tp.Tracer(scope)}
}

type tracer struct{ t trace.Tracer }

func (t tracer) StartWorkflow(ctx context.Context, w flows.StartInfo) ([]byte, func(error)) {
	attrs := []attribute.KeyValue{
		AttrInstanceID.String(w.InstanceID), AttrWorkflow.String(w.Workflow), AttrVersion.Int(w.Version),
	}
	if w.Parent != "" {
		attrs = append(attrs, AttrParent.String(w.Parent))
	}
	_, span := t.t.Start(ctx, "flows.start "+w.Workflow, trace.WithAttributes(attrs...))
	return encode(span.SpanContext()), func(err error) {
		if err != nil {
			fail(span, err)
		}
		span.End()
	}
}

func (t tracer) StartActivity(ctx context.Context, a flows.ActivityInfo) (context.Context, func(error)) {
	name := "flows.activity " + a.Activity
	if a.Compensation {
		name = "flows.compensate " + a.Activity
	}
	ctx, span := t.t.Start(underStart(ctx, a.Trace), name, trace.WithAttributes(
		AttrInstanceID.String(a.InstanceID), AttrExecutionID.Int64(a.ExecutionID), AttrActivity.String(a.Activity),
		AttrAttempt.Int(a.Attempt), AttrCompensation.Bool(a.Compensation), AttrUncertain.Bool(a.Uncertain),
	))
	return ctx, func(err error) {
		if err != nil {
			fail(span, err)
		}
		span.End()
	}
}

func (t tracer) StartWorkflowTask(ctx context.Context, w flows.WorkflowTaskInfo) (context.Context, func(flows.Status, error)) {
	ctx, span := t.t.Start(underStart(ctx, w.Trace), "flows.workflow_task "+w.Workflow, trace.WithAttributes(
		AttrInstanceID.String(w.InstanceID), AttrExecutionID.Int64(w.ExecutionID),
		AttrWorkflow.String(w.Workflow), AttrVersion.Int(w.Version),
		// A task of a compensating execution plans compensation steps.
		AttrCompensation.Bool(w.Status == flows.StatusCompensating),
	))
	return ctx, func(s flows.Status, err error) {
		if s != "" {
			span.SetAttributes(AttrStatus.String(string(s)))
		}
		switch {
		case err != nil:
			fail(span, err)
		case s == flows.StatusNeedsAttention || s == flows.StatusFailed:
			span.SetStatus(codes.Error, "execution "+string(s))
		}
		span.End()
	}
}

// fail records err on span, marking a lost lease.
func fail(span trace.Span, err error) {
	if errors.Is(err, flows.ErrLeaseLost) {
		span.SetAttributes(AttrOutcome.String("lease_lost"))
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// traceContextLen is the size of an encoded span context: a format
// version, the trace id, the span id and the trace flags.
const traceContextLen = 1 + 16 + 8 + 1

// encode returns sc as stored with an execution (nil when invalid: no
// trace to carry).
func encode(sc trace.SpanContext) []byte {
	if !sc.IsValid() {
		return nil
	}
	b := make([]byte, traceContextLen)
	tid, sid := sc.TraceID(), sc.SpanID()
	copy(b[1:17], tid[:])
	copy(b[17:25], sid[:])
	b[25] = byte(sc.TraceFlags())
	return b
}

// decode is encode's inverse; an invalid span context for anything else.
func decode(b []byte) trace.SpanContext {
	if len(b) != traceContextLen || b[0] != 0 {
		return trace.SpanContext{}
	}
	var tid trace.TraceID
	var sid trace.SpanID
	copy(tid[:], b[1:17])
	copy(sid[:], b[17:25])
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.TraceFlags(b[25]), Remote: true,
	})
}

// underStart returns ctx under the execution's start span (stored trace
// context tr); ctx unchanged without one.
func underStart(ctx context.Context, tr []byte) context.Context {
	if sc := decode(tr); sc.IsValid() {
		return trace.ContextWithRemoteSpanContext(ctx, sc)
	}
	return ctx
}

// NewObserver returns a flows.Observer recording metrics on mp (nil: the
// global provider):
//   - flows.activity.attempts (counter; flows.activity, flows.outcome,
//     flows.compensation);
//   - flows.activity.duration (histogram, seconds; same attributes);
//   - flows.runs.ended (counter; flows.workflow, flows.status).
func NewObserver(mp metric.MeterProvider) (flows.Observer, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	m := mp.Meter(scope)
	attempts, err := m.Int64Counter("flows.activity.attempts", metric.WithUnit("{attempt}"),
		metric.WithDescription("Activity and compensator attempts."))
	if err != nil {
		return nil, err
	}
	duration, err := m.Float64Histogram("flows.activity.duration", metric.WithUnit("s"),
		metric.WithDescription("Duration of activity and compensator attempts."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30, 60, 300))
	if err != nil {
		return nil, err
	}
	ended, err := m.Int64Counter("flows.runs.ended", metric.WithUnit("{run}"),
		metric.WithDescription("Executions ended or parked, by status."))
	if err != nil {
		return nil, err
	}
	return observer{attempts, duration, ended}, nil
}

type observer struct {
	attempts metric.Int64Counter
	duration metric.Float64Histogram
	ended    metric.Int64Counter
}

func (o observer) ActivityAttempt(a flows.ActivityAttempt) {
	attrs := metric.WithAttributes(AttrActivity.String(a.Activity), AttrOutcome.String(a.Outcome), AttrCompensation.Bool(a.Compensation))
	ctx := context.Background()
	o.attempts.Add(ctx, 1, attrs)
	o.duration.Record(ctx, a.Duration.Seconds(), attrs)
}

func (o observer) RunEnded(run flows.RunInfo) {
	o.ended.Add(context.Background(), 1, metric.WithAttributes(AttrWorkflow.String(run.Workflow), AttrStatus.String(string(run.Status))))
}
