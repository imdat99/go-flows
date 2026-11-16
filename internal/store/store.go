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
	// schedule id, activities, timers, children or locks in a completion
	// that ends the execution, a lock with TTL <= 0, or an activity result that is neither ActivityCompleted
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
	// KindCompensationFailed marks the reason of an execution parked because
	// a compensation failed for good; ReopenCompensation resumes only those.
	KindCompensationFailed = "compensation_failed"
	// KindNonDeterministic and KindStuck mark the reasons of an execution
	// parked because its code is wrong (its replay diverged from the
	// history, or it did not yield); ReopenParked resumes only those, once
	// fixed code is deployed.
	KindNonDeterministic = "non_deterministic"
	KindStuck            = "stuck"
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
	// Trace is the execution's trace context: opaque bytes a tracer gave
	// when it was started (nil without one), carried to continue-as-new.
	Trace []byte
}

// MaxTrace is the largest trace context a store keeps; a longer one is
// dropped (a tracer's bug must not bloat every row).
const MaxTrace = 256

// NewExecution describes an execution to create.
type NewExecution struct {
	InstanceID  string
	Workflow    string
	Version     int32
	Queue       string
	Input       []byte
	ParentID    int64
	ParentSched int32
	Trace       []byte // see Execution.Trace (at most MaxTrace bytes)
	// Taken is set by CompleteWorkflowTask on a child (Completion.Children)
	// whose instance id was in use: nothing was created, the parent gets
	// ChildFailed. Output only: reset by every completion.
	Taken bool
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
	Lease Lease
	// Execution is the execution the task is of, without its Input and
	// Output (the workflow's input is in its ExecutionStarted event; not
	// copying them spares every claim and resume): GetExecution has them.
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
	// An ending completion has none, except a continue-as-new: its
	// children run on, detached (T9.2).
	Children []NewExecution
	// ChildCancels are cancel requests for children of this execution (by
	// instance id); ids that are not active children are ignored.
	ChildCancels []string
	// CanceledActivities are schedule ids of activities whose context the
	// workflow canceled: their tasks are marked, and RenewLeases reports
	// them so the worker running them cancels them (GW-13). A task waiting
	// for a retry becomes claimable at once (its claim records the cancel).
	CanceledActivities []int32
	// Status is the execution's status after the task. A non-active status
	// ends the execution (Output, Err, ErrKinds apply). With
	// StatusNeedsAttention, Err and ErrKinds record the reason; with
	// StatusCompensating, the workflow's error.
	Status   Status
	Output   []byte
	Err      string
	ErrKinds string
	// ContinueAsNew, when set with Status == StatusContinued, creates the
	// next execution of the same instance id. Undelivered signals and cancel
	// requests move to it. Unfired timers, activity tasks and the link to
	// running children do not: their outcomes are dropped, and the children
	// run on. Any other end asks the active children to cancel (cancel by
	// tree), as Terminate does.
	ContinueAsNew *NewExecution
	// Carry are signals the workflow received but had not read, and a
	// cancel request it received, when it continued as new
	// (SignalReceived, ExecutionCancelRequested): they are delivered to the
	// next execution first, before the undelivered ones (GW-3).
	Carry []history.Event
	// CompensateChildren are schedule ids of children of this execution to
	// compensate (nested compensation). A child that completed (the
	// latest execution of its chain) is reopened: it becomes compensating
	// with an ExecutionCompensationRequested event, and when it ends the
	// parent gets CompensationCompleted for the schedule id (whatever its
	// own compensation did, or if it is terminated). Any other child
	// (failed or canceled: it compensated itself) has nothing to undo for
	// the parent: CompensationCompleted is delivered at once. Children of an
	// active execution are kept from PurgeEnded for this. Of a chain of
	// continue-as-new executions, only the last one is reopened.
	CompensateChildren []int32
	// Locks are business locks to take (LockRequested events). Each gets a
	// pending answer for this execution, visible at once: LockAcquired when
	// the lock is free, expired, or already held by this execution (then
	// extended), else LockHeld with the holder's instance id in Err (or
	// "another execution" when a concurrent request just created it).
	// Taking an expired lock from another active execution delivers
	// LockLost (its acquiring schedule id, the key as Name) to that one; an
	// expired holder that runs (a live lease or a running activity, or
	// being claimed or completed at that moment) keeps it (LockHeld). An execution's locks are extended by their TTL when it
	// or one of its activity tasks is claimed and when RenewLeases renews
	// its lease or an activity lease, so they lapse only while it neither
	// runs nor has a running activity; they are released when it ends
	// (continue-as-new included) or is terminated. A pending LockLost blocks
	// a terminal completion like a signal (ErrPendingEvents).
	Locks []LockRequest
	// KeepFor > 0 keeps the lease, extended to now + KeepFor, after a task
	// that leaves the execution running or compensating: the owner runs the next task itself (eager execution) and gets
	// its events from ResumeWorkflowTask or CompleteActivityTask. Otherwise
	// the lease is released, and the execution is sticky to its owner for
	// StickyFor (0 clears stickiness).
	KeepFor   time.Duration
	StickyFor time.Duration
	// DueKnown, with StickyFor, says the owner wakes by itself at the
	// execution's due times (its pending timers, whoever committed them)
	// that fall within DueKnown of now. The store then leaves a next due
	// time within both out of the due times it reports (DueClaimer): the
	// owner claims it, others would claim nothing (T10.3).
	DueKnown time.Duration
	// Resume, when set with KeepFor > 0 on an execution left running or
	// compensating, has the store return the next task under the kept lease
	// in the same round trip, written into *Resume (its Pending capacity
	// reused), its pending events those the completion made visible at
	// once (the answers to Locks) and any others: Resumed tells whether it
	// did (false when nothing was kept). The caller runs it as if from
	// ResumeWorkflowTask.
	Resume  *WorkflowTask
	Resumed bool
	// Leases is scratch space: CompleteWorkflowTask may return the leases
	// of the eagerly claimed activities in its capacity (a completion
	// reused across tasks then allocates none).
	Leases []Lease
}

// LockRequest asks for the business lock Key for TTL.
type LockRequest struct {
	ScheduleID int32
	Key        string
	TTL        time.Duration
	// Hold keeps the lock, up to Hold past its TTL, while an activity task
	// of the holder waits (in its queue, for a retry, or for a new lease
	// after its worker died): it does not run, but the workflow is not
	// idle. 0 lets the lock lapse after its TTL like an idle holder's.
	Hold time.Duration
	// Wait queues the request when the lock is not given (T9.8), in the
	// order of the requests. Releasing a lock fires the lock-wait timer
	// (LockWaitTimer) of its first waiter still waiting (its timer
	// pending, not parked), whose turn it then is for the lock's TTL: while it lasts, a
	// free lock is its only (others are refused, its instance id in Err).
	// WaitRetry is how long until the waiter's next attempt (its timer);
	// its place lapses once that attempt is overdue by TTL (a waiter dead).
	// A lock that lapses, or a woken waiter that leaves without it, wakes
	// nobody. A request without Wait leaves the queue.
	Wait      bool
	WaitRetry time.Duration
}

// LockWaitTimer is the name of the timer a waiter for lock key sleeps on
// between attempts (LockRequest.Wait).
func LockWaitTimer(key string) string { return lockWaitPrefix + key }

const lockWaitPrefix = "flows.lock:"

// NewActivityTask is an activity task to create.
type NewActivityTask struct {
	ScheduleID int32
	Name       string
	Queue      string
	Input      []byte
	Compensate bool // a compensation attempt instead of an invocation
	// Uncertain tells a compensator that the activity it undoes may not
	// have had its effect.
	Uncertain bool
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
	// Uncertain, for an invocation: an earlier attempt may have had its
	// effect (it timed out, panicked, was interrupted, or its holder died
	// mid-attempt; conservatively, also an eagerly claimed task whose
	// holder died before starting it), so a final failure is uncertain
	// too. For a
	// compensation: the activity it undoes may not have had its effect.
	Uncertain bool
	// CompData, for an invocation, is the compensation data that an
	// earlier attempt which may have had its effect returned (the most
	// recent one kept by RetryActivityTask; nil if none). The worker gives
	// it to the compensator when the final attempt returns none.
	CompData []byte
	Attempt  int32 // 1 for the first attempt
	// CancelRequested reports that the workflow canceled the activity
	// before this attempt: the worker records the cancel instead of running
	// it.
	CancelRequested bool
	// Trace is the trace context of the task's execution (Execution.Trace).
	Trace []byte
}

// ActivityResult ends an activity task: Event (ActivityCompleted or
// ActivityFailed, CompensationCompleted or CompensationFailed for a
// compensation task; the store sets ScheduleID and Attempt) is delivered to
// the execution.
type ActivityResult struct {
	Event history.Event
	// Resume, when set, is the owner's kept workflow lease of the same
	// execution (with another execution's lease no task is returned,
	// though that lease may be extended): in the same round trip the store extends it by ResumeFor and returns the workflow task
	// to run next (eager execution).
	Resume    *Lease
	ResumeFor time.Duration
	// Into, when set with Resume, is where the store may write the returned
	// task (and returns Into): the caller's reused buffer, its Pending
	// capacity included. A store may ignore it and allocate.
	Into *WorkflowTask
}

// ValidShape reports whether a completion is consistent in itself (see
// ErrInvalidCompletion): history sequence numbers that follow each other,
// continue-as-new fields only with StatusContinued, nothing to create in an
// ending completion, positive lock TTLs, one task per (schedule id, kind).
func ValidShape(c *Completion) bool {
	for i := 1; i < len(c.History); i++ {
		if c.History[i].Seq != c.History[i-1].Seq+1 {
			return false
		}
	}
	if c.ContinueAsNew != nil && c.Status != StatusContinued || len(c.Carry) > 0 && c.ContinueAsNew == nil {
		return false
	}
	for _, ev := range c.Carry {
		if ev.Type != history.SignalReceived && ev.Type != history.ExecutionCancelRequested {
			return false
		}
	}
	if !validEnding(c) {
		return false
	}
	for _, k := range c.Locks {
		if k.TTL <= 0 || k.Hold < 0 {
			return false
		}
	}
	return !duplicateTasks(c.Activities)
}

// validEnding reports whether an ending completion creates nothing that
// would be dropped at once: no activity, timer, lock or child compensation,
// and children only with a continue-as-new (they run on, detached; others
// would be canceled at once).
func validEnding(c *Completion) bool {
	if c.Status == "" || c.Status.Active() {
		return true
	}
	if len(c.Activities) > 0 || len(c.Timers) > 0 || len(c.Locks) > 0 || len(c.CompensateChildren) > 0 {
		return false
	}
	return len(c.Children) == 0 || c.Status == StatusContinued
}

// duplicateTasks reports two tasks for one (schedule id, kind): a scan for
// the usual few, a set beyond.
func duplicateTasks(acts []NewActivityTask) bool {
	if len(acts) <= 16 {
		for i, a := range acts {
			for _, b := range acts[:i] {
				if a.ScheduleID == b.ScheduleID && a.Compensate == b.Compensate {
					return true
				}
			}
		}
		return false
	}
	type key struct {
		sched int32
		comp  bool
	}
	seen := make(map[key]bool, len(acts))
	for _, a := range acts {
		k := key{a.ScheduleID, a.Compensate}
		if seen[k] {
			return true
		}
		seen[k] = true
	}
	return false
}

// ValidResult reports whether t can end an activity task (ActivityResult).
func ValidResult(t history.Type) bool {
	switch t {
	case history.ActivityCompleted, history.ActivityFailed, history.CompensationCompleted, history.CompensationFailed:
		return true
	}
	return false
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
	// skipped until the stickiness expires. An execution whose kept lease
	// (Completion.KeepFor) expired may be claimed with no pending events
	// (a task that delivers nothing): its holder died.
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
	// with Completion.KeepFor: the visible pending events (possibly none)
	// and the store time. The lease then has at least ttl/2 left (it is
	// extended to ttl only when less is left, to spare a write).
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
	// after delay, for another attempt. uncertain records that the attempt
	// may have had its effect (ActivityTask.Uncertain of later attempts; not
	// for compensation tasks). Claiming a task whose lease expired without a
	// result marks it so too (its holder died mid-attempt). A non-nil
	// compData replaces the kept compensation data (ActivityTask.CompData
	// of later attempts; ignored for compensation tasks); nil keeps it.
	RetryActivityTask(ctx context.Context, l Lease, delay time.Duration, uncertain bool, compData []byte) error

	// Unstick clears every execution's stickiness to owner: a stopping
	// owner hands them over at once instead of keeping them for StickyFor.
	// Stickiness is only a claim preference, so this is not a fenced write.
	Unstick(ctx context.Context, owner string) error

	// RenewLeases extends by ttl from now every workflow and activity lease
	// of owner that is still held, in one round trip. Rows locked by
	// another transaction at that moment (a completion in progress, an
	// operator's query) are skipped instead of waited for, and reported
	// busy: one slow row must not stall the renewal of the others until
	// they all look lost (saga review item 6). A lease neither renewed nor
	// busy is lost.
	RenewLeases(ctx context.Context, owner string, workflows, activities []Lease, ttl time.Duration) (Renewal, error)

	// PurgeEnded deletes up to limit executions that ended before
	// now - olderThan (but not the descendants of an active execution), with their history, pending events and tasks, and
	// returns how many it deleted. Their instance ids become free. Rows
	// locked elsewhere are skipped, so concurrent purges split the work.
	PurgeEnded(ctx context.Context, olderThan time.Duration, limit int) (int, error)

	// ReopenCompensation resumes the compensation of an execution parked
	// because a compensation failed (its ErrKinds hold
	// KindCompensationFailed): it becomes compensating again, with a
	// CompensationRetryRequested pending event. ErrNotFound for an unknown
	// id, ErrNotActive when the latest execution is not parked that way.
	ReopenCompensation(ctx context.Context, instanceID string) error
	// NoteStuck records, under the workflow lease l, that the execution's
	// workflow code ran without yielding at history position seq (the
	// task's LastSeq), and reports whether it had at that position before,
	// on any engine: then it is parked, otherwise handed back (a stalled
	// process is not wrong code). ErrLeaseLost when l is no longer held.
	NoteStuck(ctx context.Context, l Lease, seq int64) (again bool, err error)
	// ReopenParked resumes an execution parked because its code was wrong
	// (its ErrKinds hold KindNonDeterministic or KindStuck): it becomes
	// running again, its reason and stall count (NoteStuck) cleared, ready
	// to be claimed (and replayed by the code deployed since). ErrNotFound for an unknown id,
	// ErrNotActive when the latest execution is not parked that way.
	ReopenParked(ctx context.Context, instanceID string) error

	// Terminate ends the active execution of an instance id by force:
	// ExecutionTerminated is appended to its history, no workflow code or
	// compensation runs, its pending events and tasks are dropped, its
	// parent is notified, and any lease on it is lost (the holder's writes
	// fail). It returns what an end hook sees of the terminated execution
	// (its identity, StatusTerminated, reason), from the same statement
	// (no extra read). ErrNotFound / ErrNotActive as Signal.
	Terminate(ctx context.Context, instanceID, reason string) (EndInfo, error)

	// Signal delivers a SignalReceived event to the active execution of an
	// instance id: ErrNotFound if the id is unknown, ErrNotActive if its
	// latest execution ended.
	Signal(ctx context.Context, instanceID, name string, payload []byte) error
	// RequestCancel delivers an ExecutionCancelRequested event to the
	// active execution of an instance id (same errors as Signal).
	RequestCancel(ctx context.Context, instanceID string) error
}

// EndInfo describes an execution reaching an end (any non-active status
// but StatusContinued) or a park (StatusNeedsAttention), for hooks.
type EndInfo struct {
	ExecutionID int64
	InstanceID  string
	Workflow    string
	Version     int32
	Status      Status
	Err         string // the error, or the reason of a park
	ErrKinds    string
}

// Ends reports whether a completion with status s ends or parks an
// execution (what end hooks see).
func Ends(s Status) bool {
	return s == StatusNeedsAttention || s != "" && !s.Active() && s != StatusContinued
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

// Renewal is the outcome of RenewLeases: the leases that were lost (by
// lease ID) and the ones that were busy and should be retried. Every other
// lease was renewed.
type Renewal struct {
	LostWorkflows, BusyWorkflows   []int64
	LostActivities, BusyActivities []int64
	// CanceledActivities were renewed but the workflow canceled them: the
	// worker cancels their context (their outcome is still recorded).
	CanceledActivities []int64
}

// WorkKind tells what kind of task a Watch hint is about.
type WorkKind uint8

const (
	// WorkflowWork means a workflow task may be claimable.
	WorkflowWork WorkKind = iota + 1
	// ActivityWork means an activity task may be claimable.
	ActivityWork
)

// DueClaimer is implemented by stores that report, with a claim, when the
// next work of the queues becomes claimable (a future readiness: a timer, a
// retry delay, a hand-back), in the store's time: a poller sleeps until
// then instead of finding it at its next timed poll (T9.1). next <= 0: none
// known (the timed polls still cover expired leases).
type DueClaimer interface {
	ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) (tasks []WorkflowTask, next time.Duration, err error)
	ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) (tasks []ActivityTask, next time.Duration, err error)
}

// Watcher is implemented by stores that can push hints that work is ready
// (Postgres LISTEN/NOTIFY), so pollers can sleep longer between polls.
type Watcher interface {
	// Watch calls wake for each hint until ctx is done or the connection
	// fails (then it returns the error; callers restart it with backoff).
	// Hints can be missed, duplicated or spurious, and future work (timers,
	// retry delays) gets none: pollers must still poll. wake must not block
	// nor call the store: hand the hint to a buffered channel and drop it
	// when full (Postgres stops delivering, and eventually rejects commits
	// that notify, while a listener does not read its notifications).
	// Queue names must stay well under 8000 bytes (the payload limit).
	// Once listening (at the start, and again after a restart), Watch
	// wakes both kinds with an empty queue, meaning every queue: work that
	// became ready before it listened got no hint.
	Watch(ctx context.Context, wake func(kind WorkKind, queue string)) error
}
