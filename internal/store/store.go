// Package store defines the persistence contract of the engine and the
// conformance suite every implementation must pass (storetest). Two
// implementations exist: Postgres (internal/store/pg), the source of truth in
// production, and memory (internal/store/mem), for unit tests and the
// workflow tester.
//
// Guarantees every implementation provides:
//   - Fencing. Every write made on behalf of a claimed task names its lease
//     (owner and version) and fails with ErrLeaseLost when the lease moved
//     on: it was granted again (to anyone, the same owner included) or
//     released, or the task is gone. A lease that expired but was not granted
//     again is still valid: expiry only makes the task claimable (saga
//     semantics). A lease is granted only when the previous one expired or
//     was released, never to a live holder, and every grant increments the
//     version (GW-2).
//   - One clock. Lease expiry, timer visibility and timestamps come from the
//     store's clock: Postgres now() (GW-5), or the memory store's injected
//     clock.
//   - Atomic checkpoints. CompleteWorkflowTask applies everything a workflow
//     task produced, or nothing.
//   - At most one active execution per instance id (GW-1). An instance id
//     is an idempotency key: it stays reserved after its execution ends,
//     until retention purges it.
//   - Signals and cancel requests are never accepted and then lost (GW-3).
//     They reach the active execution. A completion that would end it while
//     such events are pending and undelivered fails with ErrPendingEvents,
//     so that the workflow sees them first. Continue-as-new carries them to
//     the next execution.
//   - Pending events are delivered in (visibility time, arrival) order.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/imdat99/go-flows/internal/history"
)

var (
	// ErrLeaseLost reports a write whose lease is no longer held: it expired
	// and was granted to someone else, or was released.
	ErrLeaseLost = errors.New("store: lease lost")
	// ErrNotFound reports an unknown instance.
	ErrNotFound = errors.New("store: instance not found")
	// ErrNotActive reports a signal or cancel sent to an instance whose
	// latest execution has ended.
	ErrNotActive = errors.New("store: instance not active")
	// ErrInstanceConflict reports an instance id already used by another
	// workflow (the id is an idempotency key).
	ErrInstanceConflict = errors.New("store: instance id used by another workflow")
	// ErrInvalidCompletion reports an executor bug, and nothing is applied.
	// Causes: history sequence numbers that do not continue the history,
	// ContinueAsNew without StatusContinued, two activity tasks for one
	// schedule id, activities, timers or children in a completion that ends
	// the execution, or an activity result that is neither ActivityCompleted
	// nor ActivityFailed.
	ErrInvalidCompletion = errors.New("store: invalid completion")
	// ErrPendingEvents rejects a completion that would end the execution
	// while signals or cancel requests it has not seen are pending. Nothing
	// is applied; the executor resumes its lease (ResumeWorkflowTask) and
	// runs a new task with them (GW-3). A steady stream of signals can delay
	// the end indefinitely: this is the price of never losing one.
	ErrPendingEvents = errors.New("store: undelivered signals or cancel requests")
)

// Error kinds the store adds to the ChildFailed events it delivers to a
// parent, after the child's own kinds.
const (
	// KindInstanceExists means the child's instance id was already in use.
	KindInstanceExists = "instance_exists"
	// KindCanceled means the child ended canceled.
	KindCanceled = "canceled"
	// KindTerminated means the child was terminated.
	KindTerminated = "terminated"
	// KindCompensated means the child failed and was compensated.
	KindCompensated = "compensated"
)

// Status of an execution. Values are stored: never change them.
type Status string

const (
	// StatusRunning means the execution is in progress.
	StatusRunning Status = "running"
	// StatusCompensating means its compensators are running.
	StatusCompensating Status = "compensating"
	// StatusNeedsAttention means it is parked for an operator
	// (non-determinism, compensation retries exhausted).
	StatusNeedsAttention Status = "needs_attention"
	// StatusCompleted means it ended with an output.
	StatusCompleted Status = "completed"
	// StatusFailed means it ended with an error and nothing to compensate.
	StatusFailed Status = "failed"
	// StatusCompensated means it failed and its compensators all ran.
	StatusCompensated Status = "compensated"
	// StatusCanceled means it ended after a cancel request.
	StatusCanceled Status = "canceled"
	// StatusTerminated means it was ended by force.
	StatusTerminated Status = "terminated"
	// StatusContinued means it ended with continue-as-new: a newer execution
	// of the same instance id took over.
	StatusContinued Status = "continued"
)

// Active reports whether executions in this status still hold their
// instance id exclusively.
func (s Status) Active() bool {
	return s == StatusRunning || s == StatusCompensating || s == StatusNeedsAttention
}

// Execution is one execution of a workflow instance. ContinueAsNew ends an
// execution and creates the next one with the same InstanceID.
type Execution struct {
	ID          int64 // store-assigned, unique across instances
	InstanceID  string
	Workflow    string
	Version     int32
	Queue       string
	Status      Status
	ParentID    int64 // execution id of the parent, 0 for a root workflow
	ParentSched int32 // schedule id of the ChildScheduled event in the parent
	Input       []byte
	Output      []byte
	Err         string
	ErrKinds    string
	CreatedAt   time.Time
	EndedAt     time.Time // zero while active
}

// NewExecution describes an execution to create.
type NewExecution struct {
	InstanceID  string
	Workflow    string
	Version     int32
	Queue       string
	Input       []byte
	ParentID    int64
	ParentSched int32
}

// Lease identifies the ownership of a claimed task, which every write made
// for it must still hold.
type Lease struct {
	ID      int64 // execution id (workflow tasks) or activity task id
	Owner   string
	Version int64
}

// WorkflowTask is a claimed execution with the events to deliver to it.
type WorkflowTask struct {
	Lease     Lease
	Execution Execution
	// LastSeq is the sequence number of the last history event; a cached
	// executor whose history ends elsewhere must reload (GW-9).
	LastSeq int64
	// Pending are the events to deliver, in (visibility time, arrival)
	// order. Their IDs are passed back in Completion.Consumed.
	Pending []PendingEvent
	// Now is the store time at the claim: the time of the TaskStarted event.
	Now time.Time
}

// PendingEvent is an event waiting for the next workflow task.
type PendingEvent struct {
	ID    int64
	Event history.Event
}

// Completion is everything a workflow task produced, applied atomically by
// CompleteWorkflowTask.
type Completion struct {
	// History are the events appended to the history, Seq assigned by the
	// executor and continuing from WorkflowTask.LastSeq.
	History []history.Event
	// Consumed are the pending events delivered by this task.
	Consumed []int64
	// Activities are the activity tasks to create (one per ActivityScheduled
	// event).
	Activities []NewActivityTask
	// Timers are pending TimerFired events, visible at their At.
	Timers []history.Event
	// CanceledTimers are schedule ids whose pending TimerFired must go.
	CanceledTimers []int32
	// Children are executions to create (child workflows); a child id
	// already in use yields a ChildFailed pending event for this execution.
	Children []NewExecution
	// ChildCancels are cancel requests for children of this execution (by
	// instance id); ids that are not active children are ignored.
	ChildCancels []string
	// Status is the execution's status after the task. A non-active status
	// ends the execution (Output, Err, ErrKinds apply).
	Status   Status
	Output   []byte
	Err      string
	ErrKinds string
	// ContinueAsNew, when set with Status == StatusContinued, creates the
	// next execution of the same instance id. Undelivered signals and cancel
	// requests move to it. Unfired timers, activity tasks and the link to
	// running children do not: their outcomes are dropped.
	ContinueAsNew *NewExecution
	// KeepFor > 0 keeps the lease, extended to now + KeepFor, after a task
	// that leaves the execution running or compensating: the owner runs the next task itself (eager execution) and gets
	// its events from ResumeWorkflowTask or CompleteActivityTask. Otherwise
	// the lease is released, and the execution is sticky to its owner for
	// StickyFor (0 clears stickiness).
	KeepFor   time.Duration
	StickyFor time.Duration
}

// NewActivityTask is an activity task to create.
type NewActivityTask struct {
	ScheduleID int32
	Name       string
	Queue      string
	Input      []byte
	Compensate bool // a compensation attempt instead of an invocation
	// ClaimFor, when > 0, creates the task already claimed by the
	// completing owner for this long (eager execution). The resulting
	// leases are returned by CompleteWorkflowTask.
	ClaimFor time.Duration
}

// ActivityTask is a claimed activity task.
type ActivityTask struct {
	Lease       Lease
	ExecutionID int64
	InstanceID  string
	ScheduleID  int32
	Name        string
	Queue       string
	Input       []byte
	Compensate  bool
	Attempt     int32 // 1 for the first attempt
}

// ActivityResult ends an activity task: Event (ActivityCompleted or
// ActivityFailed; the store sets ScheduleID and Attempt) is delivered to the
// execution.
type ActivityResult struct {
	Event history.Event
	// Resume, when set, is the owner's kept workflow lease of the same
	// execution (another execution's lease is ignored): in the same round
	// trip the store extends it by ResumeFor and returns the workflow task
	// to run next (eager execution).
	Resume    *Lease
	ResumeFor time.Duration
}

// Store is the persistence contract of the engine. Methods are safe for
// concurrent use.
type Store interface {
	// Now returns the store's clock.
	Now(ctx context.Context) (time.Time, error)

	// CreateExecution creates the first execution of an instance, with its
	// ExecutionStarted pending event, unless the instance id exists. Then it
	// returns the latest execution of that id and created == false, or
	// ErrInstanceConflict when that execution belongs to another workflow.
	CreateExecution(ctx context.Context, e NewExecution) (Execution, bool, error)
	// GetExecution returns the latest execution of an instance id.
	GetExecution(ctx context.Context, instanceID string) (Execution, error)

	// ClaimWorkflowTasks claims up to limit executions of the given queues
	// that have visible pending events and no live lease, preferring the
	// oldest ready ones, for ttl. Executions sticky to another owner are
	// skipped until the stickiness expires.
	ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]WorkflowTask, error)
	// LoadHistory returns the history events with Seq > afterSeq.
	LoadHistory(ctx context.Context, executionID, afterSeq int64) ([]history.Event, error)
	// CompleteWorkflowTask applies c atomically under lease l. It returns
	// the leases of eagerly claimed activity tasks, in the order of
	// c.Activities entries that asked for one. It fails with
	// ErrPendingEvents or ErrInvalidCompletion without applying anything.
	CompleteWorkflowTask(ctx context.Context, l Lease, c *Completion) ([]Lease, error)
	// ExtendWorkflowTask extends lease l by ttl from now.
	ExtendWorkflowTask(ctx context.Context, l Lease, ttl time.Duration) error
	// ReleaseWorkflowTask gives up lease l without changes and clears
	// stickiness; the execution is claimable again after delay.
	ReleaseWorkflowTask(ctx context.Context, l Lease, delay time.Duration) error
	// ResumeWorkflowTask returns the next workflow task under a lease kept
	// with Completion.KeepFor, extended by ttl: the visible pending events
	// (possibly none) and the store time.
	ResumeWorkflowTask(ctx context.Context, l Lease, ttl time.Duration) (WorkflowTask, error)

	// ClaimActivityTasks claims up to limit visible, unclaimed (or expired)
	// activity tasks of the given queues, oldest first, for ttl. Each claim
	// increments Attempt.
	ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]ActivityTask, error)
	// ExtendActivityTask extends lease l by ttl from now.
	ExtendActivityTask(ctx context.Context, l Lease, ttl time.Duration) error
	// CompleteActivityTask ends the task under lease l and delivers r.Event
	// to its execution (if the execution is still active; otherwise the
	// result is dropped). With r.Resume it also resumes that workflow lease
	// and returns the task (see ActivityResult). If that lease was lost, the
	// result is still recorded and the returned task is nil.
	CompleteActivityTask(ctx context.Context, l Lease, r ActivityResult) (*WorkflowTask, error)
	// RetryActivityTask releases lease l and makes the task visible again
	// after delay, for another attempt.
	RetryActivityTask(ctx context.Context, l Lease, delay time.Duration) error

	// Signal delivers a SignalReceived event to the active execution of an
	// instance id: ErrNotFound if the id is unknown, ErrNotActive if its
	// latest execution ended.
	Signal(ctx context.Context, instanceID, name string, payload []byte) error
	// RequestCancel delivers an ExecutionCancelRequested event to the
	// active execution of an instance id (same errors as Signal).
	RequestCancel(ctx context.Context, instanceID string) error
}

// ChildOutcome is the event the store delivers to the parent of an ended
// child execution: ChildCompleted with the output, or ChildFailed with the
// child's error and kinds plus a kind for the way it ended.
func ChildOutcome(child Execution) history.Event {
	ev := history.Event{ScheduleID: child.ParentSched, Ref: child.InstanceID}
	if child.Status == StatusCompleted {
		ev.Type, ev.Payload = history.ChildCompleted, child.Output
		return ev
	}
	ev.Type, ev.Err, ev.ErrKinds = history.ChildFailed, child.Err, child.ErrKinds
	if ev.Err == "" {
		ev.Err = "child workflow " + string(child.Status)
	}
	kind := ""
	switch child.Status {
	case StatusCanceled:
		kind = KindCanceled
	case StatusTerminated:
		kind = KindTerminated
	case StatusCompensated:
		kind = KindCompensated
	case StatusRunning, StatusCompensating, StatusNeedsAttention, StatusCompleted, StatusFailed, StatusContinued:
	}
	if kind != "" {
		if ev.ErrKinds != "" {
			ev.ErrKinds += ","
		}
		ev.ErrKinds += kind
	}
	return ev
}
