package flows

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/text"
)

// WorkflowFunc is a workflow: deterministic code that calls activities,
// timers, signals and children through c.
type WorkflowFunc[I, O any] func(c Ctx, in I) (O, error)

// WorkflowOption configures a workflow.
type WorkflowOption func(*workflowConfig)

type workflowConfig struct{ queue string }

// WithWorkflowQueue runs the workflow's tasks on queue.
func WithWorkflowQueue(queue string) WorkflowOption {
	mustQueue(queue)
	return func(c *workflowConfig) { c.queue = queue }
}

// Workflow is a workflow definition.
type Workflow[I, O any] struct {
	name    string
	version int32
	queue   string
	in      codec[I]
	out     codec[O]
}

// DefineWorkflow defines the given version (>= 1) of the workflow called name.
// Executions record name and version: deploy a changed workflow as a new
// version, and keep the old definition until its executions ended (an
// instance that does not know an execution's version hands it back to one
// that does). Define workflows at package level.
func DefineWorkflow[I, O any](name string, version int, fn WorkflowFunc[I, O], opts ...WorkflowOption) *Workflow[I, O] {
	if version < 1 || version > math.MaxInt32 { // checked before the int32 conversion truncates it
		panic(fmt.Sprintf("flows: workflow %q: version %d out of range [1, %d]", name, version, math.MaxInt32))
	}
	cfg := workflowConfig{queue: DefaultQueue}
	for _, o := range opts {
		o(&cfg)
	}
	w := &Workflow[I, O]{name: name, version: int32(version), queue: cfg.queue, in: newCodec[I](), out: newCodec[O]()}
	registerWorkflow(executor.Workflow{Name: name, Version: w.version, Run: func(cc coro.Context, raw []byte) ([]byte, error) {
		in, err := w.in.decode(raw)
		if err != nil {
			return nil, Permanent(fmt.Errorf("decode input of %s: %w", name, err))
		}
		out, err := callWorkflow(fn, Ctx{cc}, in)
		if err != nil {
			return nil, err
		}
		b, err := w.out.encode(out)
		if err != nil {
			return nil, Permanent(fmt.Errorf("encode output of %s: %w", name, err))
		}
		return b, nil
	}})
	return w
}

// callWorkflow runs fn; a panic of ErrNotInWorkflow misuse or of user code
// becomes the workflow's error (the executor recovers panics as well; this
// gives them the ErrPanic kind).
func callWorkflow[I, O any](fn WorkflowFunc[I, O], c Ctx, in I) (out O, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && errors.Is(e, ErrNotInWorkflow) {
				err = e
				return
			}
			panic(r) // the executor records it, with the stack
		}
	}()
	return fn(c, in)
}

// Name returns the workflow's name.
func (w *Workflow[I, O]) Name() string { return w.name }

// Version returns the workflow's version.
func (w *Workflow[I, O]) Version() int { return int(w.version) }

// Start starts an execution of the workflow with instance id instanceID,
// an idempotency key: if the id was used already (by an execution of this
// workflow, running or ended, until retention purges it), Start returns a
// handle on that execution and starts nothing. ErrInstanceConflict when
// the id belongs to another workflow.
func (w *Workflow[I, O]) Start(ctx context.Context, e *Engine, instanceID string, in I) (*Handle[O], error) {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return nil, err
	}
	b, err := w.in.encode(in)
	if err != nil {
		return nil, fmt.Errorf("flows: encode input of %s: %w", w.name, err)
	}
	if _, _, err := e.st.CreateExecution(ctx, store.NewExecution{
		InstanceID: instanceID, Workflow: w.name, Version: w.version, Queue: w.queue, Input: b,
	}); err != nil {
		return nil, err
	}
	return w.Handle(e, instanceID), nil
}

// Run starts the workflow (see Start) and waits for its result.
func (w *Workflow[I, O]) Run(ctx context.Context, e *Engine, instanceID string, in I) (O, error) {
	h, err := w.Start(ctx, e, instanceID, in)
	if err != nil {
		var zero O
		return zero, err
	}
	return h.Wait(ctx)
}

// Handle returns a handle on the executions of instance id instanceID.
func (w *Workflow[I, O]) Handle(e *Engine, instanceID string) *Handle[O] {
	return &Handle[O]{e: e, id: instanceID, wf: w.name, out: w.out}
}

// Child starts the workflow as a child of the running workflow, with
// instance id instanceID, and returns its future. Canceling c cancels the
// child. A child whose id is already in use fails with ErrInstanceExists.
//
// When the parent fails (or is canceled) and compensates, a child that
// completed is compensated too, in reverse order with the parent's own
// activities: it is reopened and undoes its own activities and children,
// without its locks (released when it completed). Of a child that continued
// as new, only the last execution is compensated; activities a child
// started without awaiting them are not. A child still running is canceled
// (and compensates itself), and the parent waits for it; a child that
// ignores the cancel and completes afterwards is not compensated.
func (w *Workflow[I, O]) Child(c Ctx, instanceID string, in I) *Future[O] {
	if err := text.CheckName(ErrInvalidArgument, "instance id", instanceID); err != nil {
		return &Future[O]{err: Permanent(err)}
	}
	b, err := w.in.encode(in)
	if err != nil {
		return &Future[O]{err: Permanent(fmt.Errorf("encode input of %s: %w", w.name, err))}
	}
	return &Future[O]{res: c.state().StartChild(c.c, instanceID, w.name, w.version, w.queue, b), cd: w.out}
}

// ContinueAsNew ends the running execution and starts the next one of the
// same instance with in (return its error from the workflow function): a
// long-lived workflow keeps its history short this way. Signals not read
// yet (delivered or not) move to the next execution, in order; children the
// execution started run on, detached from it, including those started in
// the same task (a child whose id is taken then fails unseen: nothing
// awaits it). Detached children are not compensated with the chain (a
// compensation reaches the latest execution only). Activities started in
// the same task without being awaited are not started (the engine logs a
// warning). Any other end asks running children to cancel, and starts
// neither the children nor the activities started in its last task
// without being awaited (logged too).
func ContinueAsNew[I any](c Ctx, in I) error {
	b, err := newCodec[I]().encode(in)
	if err != nil {
		return Permanent(fmt.Errorf("flows: encode continue-as-new input: %w", err))
	}
	_ = c.state()
	return &executor.ContinueAsNewError{Input: b}
}

// Status of a workflow execution.
type Status = store.Status

// Execution statuses.
const (
	StatusRunning        = store.StatusRunning
	StatusCompensating   = store.StatusCompensating
	StatusNeedsAttention = store.StatusNeedsAttention
	StatusCompleted      = store.StatusCompleted
	StatusFailed         = store.StatusFailed
	StatusCompensated    = store.StatusCompensated
	StatusCanceled       = store.StatusCanceled
	StatusTerminated     = store.StatusTerminated
)

// Handle follows the executions of one instance id.
type Handle[O any] struct {
	e   *Engine
	id  string
	wf  string // the workflow the handle decodes outputs of
	out codec[O]
}

// InstanceID returns the handle's instance id.
func (h *Handle[O]) InstanceID() string { return h.id }

// Status returns the status of the instance's latest execution.
func (h *Handle[O]) Status(ctx context.Context) (Status, error) {
	e, err := h.e.st.GetExecution(ctx, h.id)
	return e.Status, err
}

// Wait waits for the instance to end (following continue-as-new) and returns
// its output, or a *RunError for any other end (errors.Is matches the
// recorded kinds, and ErrCanceled, ErrTerminated or ErrCompensated for the
// way it ended). A failed or canceled workflow is compensated before it
// ends. An execution parked as needs_attention ends the wait with a
// *RunError too, although it can resume once fixed: errors.Is
// ErrNonDeterministic or ErrStuck (wrong workflow code: Engine.Resume), or
// ErrCompensationFailed (Engine.RetryCompensation).
func (h *Handle[O]) Wait(ctx context.Context) (O, error) {
	var zero O
	delay := 5 * time.Millisecond
	for {
		e, err := h.e.st.GetExecution(ctx, h.id)
		if err != nil {
			return zero, err
		}
		if e.Workflow != h.wf { // its output is not an O
			return zero, fmt.Errorf("%w: instance %s runs workflow %s, not %s", ErrInstanceConflict, h.id, e.Workflow, h.wf)
		}
		switch {
		case e.Status == store.StatusCompleted:
			return h.out.decode(e.Output)
		case e.Status == store.StatusNeedsAttention || !e.Status.Active() && e.Status != store.StatusContinued:
			cause := recorded(e.Err, e.ErrKinds, false)
			switch e.Status { // the way it ended is a kind too
			case StatusTerminated:
				cause.kinds = append(cause.kinds, store.KindTerminated)
			case StatusCanceled:
				cause.kinds = append(cause.kinds, "canceled")
			case StatusCompensated:
				cause.kinds = append(cause.kinds, store.KindCompensated)
			case StatusNeedsAttention, StatusFailed, StatusRunning, StatusCompensating,
				StatusCompleted, store.StatusContinued:
			}
			return zero, &RunError{InstanceID: h.id, Status: e.Status, Cause: cause}
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return zero, ctx.Err()
		case <-t.C:
		}
		delay = min(2*delay, 500*time.Millisecond)
	}
}
