// Package otelflows instruments go-flows engines with OpenTelemetry:
// traces (a span per activity attempt, the parent of the spans the activity
// makes, and a span per workflow task) and metrics (activity attempts and
// their durations, executions ended).
//
//	tr := otelflows.NewTracer(nil)          // the global TracerProvider
//	obs, err := otelflows.NewObserver(nil)  // the global MeterProvider
//	engine, err := flows.New(pool, flows.Options{Tracer: tr, Observer: obs})
//
// It is a separate module, so go-flows itself does not depend on
// OpenTelemetry; an engine without a Tracer or Observer pays nothing.
//
// One trace per execution: a workflow's tasks and attempts run at
// different times, possibly in different processes, and no trace context
// is stored with an execution. Instead the trace id, and the span id of a
// span context standing for the execution, are derived from the execution
// (its instance id and execution id): every task and attempt span, in any
// process, is a child of that span context, so they share one trace (an
// attempt whose lease was lost, and the attempt that replaced it, side by
// side). The execution's span is never emitted itself (backends show the
// trace's root as missing). Continue-as-new starts another execution, so
// another trace; the flows.instance_id attribute ties them.
//
// That span context is a remote parent marked sampled, so a ParentBased
// sampler keeps every span of an execution by default. To sample whole
// executions, use its remote-parent option with a ratio of trace ids,
// which is consistent across processes:
//
//	sdktrace.ParentBased(root, sdktrace.WithRemoteParentSampled(sdktrace.TraceIDRatioBased(0.1)))
//
// Spans of an attempt or task whose lease was lost (another engine runs it
// again) end with an error status and flows.outcome "lease_lost".
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

func (t tracer) StartActivity(ctx context.Context, a flows.ActivityInfo) (context.Context, func(error)) {
	name := "flows.activity " + a.Activity
	if a.Compensation {
		name = "flows.compensate " + a.Activity
	}
	ctx, span := t.t.Start(inExecution(ctx, a.InstanceID, a.ExecutionID), name, trace.WithAttributes(
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

// fail records err on span, marking a lost lease.
func fail(span trace.Span, err error) {
	if errors.Is(err, flows.ErrLeaseLost) {
		span.SetAttributes(AttrOutcome.String("lease_lost"))
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// inExecution returns ctx under the span context standing for execution
// id of instance, replacing any span ctx holds (the engine's own context
// is not the execution's); ctx unchanged for id 0, an unknown execution.
func inExecution(ctx context.Context, instance string, id int64) context.Context {
	if id == 0 {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, executionSpanContext(instance, id))
}

// executionSpanContext derives the span context of an execution from its
// identity (FNV-1a with distinct seeds: no allocation, the same in every
// process). The execution id is unique in its store; the instance id
// separates executions of different stores.
func executionSpanContext(instance string, id int64) trace.SpanContext {
	var tid trace.TraceID
	var sid trace.SpanID
	put := func(b []byte, v uint64) {
		for i := range 8 {
			b[i] = byte(v >> (56 - 8*i))
		}
	}
	put(tid[:8], identityHash(1, instance, id))
	put(tid[8:], identityHash(2, instance, id))
	put(sid[:], identityHash(3, instance, id))
	if !tid.IsValid() { // all zero: practically never
		tid[15] = 1
	}
	if !sid.IsValid() {
		sid[7] = 1
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true,
	})
}

// identityHash is FNV-1a 64 over seed, instance and id.
func identityHash(seed byte, instance string, id int64) uint64 {
	const prime = 1099511628211
	h := uint64(14695981039346656037)
	h = (h ^ uint64(seed)) * prime
	for i := range len(instance) {
		h = (h ^ uint64(instance[i])) * prime
	}
	h = (h ^ 0xff) * prime // end of the instance id (the seed and id are fixed-width)
	for i := range 8 {
		h = (h ^ uint64(byte(uint64(id)>>(8*i)))) * prime
	}
	return h
}

func (t tracer) StartWorkflowTask(ctx context.Context, w flows.WorkflowTaskInfo) func(flows.Status, error) {
	_, span := t.t.Start(inExecution(ctx, w.InstanceID, w.ExecutionID), "flows.workflow_task "+w.Workflow, trace.WithAttributes(
		AttrInstanceID.String(w.InstanceID), AttrExecutionID.Int64(w.ExecutionID),
		AttrWorkflow.String(w.Workflow), AttrVersion.Int(w.Version),
		// A task of a compensating execution plans compensation steps.
		AttrCompensation.Bool(w.Status == flows.StatusCompensating),
	))
	return func(s flows.Status, err error) {
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
