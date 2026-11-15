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
// Spans are roots, tied together by the flows.instance_id attribute: a
// workflow's tasks and attempts run at different times, possibly in
// different processes, and no trace context is stored with an execution.
package otelflows

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	flows "github.com/imdat99/go-flows"
)

const scope = "github.com/imdat99/go-flows/otelflows"

// Attribute keys.
const (
	AttrInstanceID   = attribute.Key("flows.instance_id")
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
	ctx, span := t.t.Start(ctx, name, trace.WithAttributes(
		AttrInstanceID.String(a.InstanceID), AttrActivity.String(a.Activity), AttrAttempt.Int(a.Attempt),
		AttrCompensation.Bool(a.Compensation), AttrUncertain.Bool(a.Uncertain),
	))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (t tracer) StartWorkflowTask(ctx context.Context, w flows.WorkflowTaskInfo) func(flows.Status, error) {
	_, span := t.t.Start(ctx, "flows.workflow_task "+w.Workflow, trace.WithAttributes(
		AttrInstanceID.String(w.InstanceID), AttrWorkflow.String(w.Workflow), AttrVersion.Int(w.Version),
	))
	return func(s flows.Status, err error) {
		if s != "" {
			span.SetAttributes(AttrStatus.String(string(s)))
		}
		switch {
		case err != nil:
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
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
