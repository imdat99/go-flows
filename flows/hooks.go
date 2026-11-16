package flows

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/worker"
)

// Ported from saga (hooks.go, same author), for executions.

// RunInfo describes an execution reaching an end or a park.
type RunInfo struct {
	InstanceID string
	Workflow   string
	Version    int
	// Status is completed, failed, compensated, canceled, terminated or
	// needs_attention (continue-as-new is not an end).
	Status Status
	// Error is the workflow's error (or the termination reason, or the
	// reason of a park); ErrorKinds its recorded kinds, comma separated.
	Error, ErrorKinds string
}

// RunHook observes executions ending. RunEnded is called inside the
// transaction that records the end or park, under the execution's lease,
// so whatever it writes commits atomically with the status; returning an
// error (or panicking) rolls the transition back, and the task is retried
// later. A transactional outbox can use it to release (completed) or drop
// (compensated) the messages an execution staged.
//
// It runs on the engine's path (and makes ends use an explicit transaction
// instead of the pipelined fast path): keep it to a few statements. It must
// not commit or roll back tx (use tx.Begin for a savepoint). After an
// error the execution's task is handed back, and runs again a few seconds
// later (the end is not lost). On the memory store of tests, tx is nil.
type RunHook interface {
	RunEnded(ctx context.Context, tx pgx.Tx, run RunInfo) error
}

// Observer receives engine measurements, e.g. to feed metrics. It is
// called after the fact, outside transactions; its methods must be fast and
// must not block (hand off to a channel or update counters). Panics are
// recovered and logged.
type Observer interface {
	// ActivityAttempt follows each attempt of an activity or a compensator
	// (an attempt whose recording fails because its lease was lost may be
	// reported again by the instance that runs it next).
	ActivityAttempt(a ActivityAttempt)
	// RunEnded follows a committed end or park.
	RunEnded(run RunInfo)
}

// ActivityAttempt describes one attempt of an activity or compensator.
type ActivityAttempt struct {
	InstanceID   string
	Activity     string
	Attempt      int
	Compensation bool
	// Outcome is "completed", "failed" or "retrying".
	Outcome  string
	Duration time.Duration
	Err      error
}

func runInfo(e store.EndInfo) RunInfo {
	return RunInfo{
		InstanceID: e.InstanceID, Workflow: e.Workflow, Version: int(e.Version),
		Status: e.Status, Error: e.Err, ErrorKinds: e.ErrKinds,
	}
}

// observer adapts an Observer to the worker.
type observer struct{ o Observer }

var _ worker.Observer = observer{}

func (o observer) ActivityAttempt(t *store.ActivityTask, outcome string, d time.Duration, err error) {
	o.o.ActivityAttempt(ActivityAttempt{
		InstanceID: t.InstanceID, Activity: t.Name, Attempt: int(t.Attempt), Compensation: t.Compensate,
		Outcome: outcome, Duration: d, Err: err,
	})
}

func (o observer) Ended(e store.EndInfo) { o.o.RunEnded(runInfo(e)) }

// Tracer wraps engine work in spans; package otelflows implements it with
// OpenTelemetry. Unset, it costs nothing. Its panics are recovered and
// logged. Workflow code itself is replayed and not traced: a span covers a
// whole workflow task.
//
// The trace context of an execution is taken when it is started:
// StartWorkflow, from the caller's context (Workflow.Start) or from the
// starting workflow task's (a child), returns it as opaque bytes, stored
// with the execution and handed back to every task and attempt of it, in
// any process (WorkflowTaskInfo.Trace, ActivityInfo.Trace); continue-as-new
// keeps it. The engine never interprets them. An attempt or task whose
// lease was lost (another engine runs it again) ends with an error
// matching ErrLeaseLost.
type Tracer interface {
	// StartWorkflow starts an execution: by Workflow.Start (ctx is the
	// caller's) or by a workflow starting a child (ctx is the context
	// StartWorkflowTask returned for the task starting it). It returns the
	// trace context to store with the execution (nil: none; over
	// MaxTraceContext bytes it is dropped) and end, called once the start
	// is stored (nil) or failed. Starting an instance id that exists
	// stores nothing: the existing execution keeps its trace context.
	StartWorkflow(ctx context.Context, w StartInfo) (trace []byte, end func(err error))
	// StartActivity starts an attempt of an activity or compensator. The
	// returned context is the attempt's: spans the activity makes are its
	// children. It must derive from ctx (context.WithValue), or the
	// activity is no longer interrupted by a lost lease, a stop or a cancel.
	// end gets the attempt's error (nil when it completed).
	StartActivity(ctx context.Context, a ActivityInfo) (context.Context, func(err error))
	// StartWorkflowTask starts a workflow task (a run of workflow code up
	// to its next wait, or a compensation step). end gets the status the
	// task committed ("" when it still runs, or nothing was stored;
	// needs_attention for a park) and the error that ended it early (handed
	// back to another instance, the completion refused, a park's reason).
	// The returned context is the task's: children the task starts get it
	// (StartWorkflow).
	StartWorkflowTask(ctx context.Context, w WorkflowTaskInfo) (context.Context, func(status Status, err error))
}

// MaxTraceContext is the largest trace context an execution keeps
// (Tracer.StartWorkflow).
const MaxTraceContext = store.MaxTrace

// StartInfo describes an execution being started.
type StartInfo struct {
	InstanceID string
	Workflow   string
	Version    int
	// Parent is the instance id of the workflow starting it as a child,
	// "" for Workflow.Start.
	Parent string
}

// ActivityInfo describes an activity attempt.
type ActivityInfo struct {
	InstanceID string
	// ExecutionID identifies the execution (one run of the instance id;
	// continue-as-new starts another), as WorkflowTaskInfo.ExecutionID.
	ExecutionID  int64
	Activity     string
	Attempt      int
	Compensation bool
	// Uncertain: for an invocation, an earlier attempt may have had its
	// effect; for a compensation, the activity may not have had one.
	Uncertain bool
	// Trace is the execution's trace context (Tracer.StartWorkflow), nil
	// when none was stored. It must not be modified.
	Trace []byte
}

// WorkflowTaskInfo describes a workflow task.
type WorkflowTaskInfo struct {
	InstanceID string
	// ExecutionID identifies the execution (unique in its store).
	ExecutionID int64
	Workflow    string
	Version     int
	// Status is the execution's status when the task starts (running or
	// compensating).
	Status Status
	// Trace is the execution's trace context, as ActivityInfo.Trace.
	Trace []byte
}

// tracer adapts a Tracer to the worker.
type tracer struct{ t Tracer }

var _ worker.Tracer = tracer{}

func (t tracer) StartAttempt(ctx context.Context, a *store.ActivityTask) (context.Context, func(error)) {
	return t.t.StartActivity(ctx, ActivityInfo{
		InstanceID: a.InstanceID, ExecutionID: a.ExecutionID, Activity: a.Name, Attempt: int(a.Attempt),
		Compensation: a.Compensate, Uncertain: a.Uncertain, Trace: a.Trace,
	})
}

func (t tracer) StartTask(ctx context.Context, e *store.Execution) (context.Context, func(store.Status, error)) {
	tctx, end := t.t.StartWorkflowTask(ctx, WorkflowTaskInfo{
		InstanceID: e.InstanceID, ExecutionID: e.ID, Workflow: e.Workflow, Version: int(e.Version),
		Status: e.Status, Trace: e.Trace,
	})
	if end == nil {
		return tctx, nil
	}
	return tctx, end
}

func (t tracer) StartChild(ctx context.Context, parent *store.Execution, ne *store.NewExecution) func(error) {
	tr, end := t.t.StartWorkflow(ctx, StartInfo{
		InstanceID: ne.InstanceID, Workflow: ne.Workflow, Version: int(ne.Version), Parent: parent.InstanceID,
	})
	ne.Trace = keptTrace(tr)
	return end
}

// keptTrace is tr, or nil when too long to store.
func keptTrace(tr []byte) []byte {
	if len(tr) > store.MaxTrace {
		return nil
	}
	return tr
}

// startTraced starts ne under the engine's tracer (if any): its trace
// context goes into ne, end (never nil) follows the start. The tracer's
// panics are recovered: the start goes on untraced.
func (e *Engine) startTraced(ctx context.Context, ne *store.NewExecution) (end func(error)) {
	end = func(error) {}
	if e.tracer == nil {
		return end
	}
	defer func() {
		if r := recover(); r != nil {
			ne.Trace, end = nil, func(error) {}
		}
	}()
	tr, tend := e.tracer.StartWorkflow(ctx, StartInfo{InstanceID: ne.InstanceID, Workflow: ne.Workflow, Version: int(ne.Version)})
	ne.Trace = keptTrace(tr)
	if tend != nil {
		end = func(err error) {
			defer func() { _ = recover() }() // a broken tracer must not fail Start
			tend(err)
		}
	}
	return end
}
