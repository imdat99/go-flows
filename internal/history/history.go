// Package history defines the events a workflow execution is replayed from.
//
// Ported in spirit from go-workflows backend/history (MIT, see NOTICE), with
// one change of shape. There, an event is an envelope whose attributes are an
// interface value serialized as JSON. Here, an Event is a flat struct with
// typed fields that map one to one onto columns of the history and
// pending_event tables:
//   - nothing is boxed;
//   - payloads are the user's encoded bytes, stored as bytea without base64;
//   - the store binds fields directly, so there is no envelope codec.
package history

import (
	"strconv"
	"time"
)

// Type is the kind of an event. Values are stored in the database: never
// renumber, only append.
type Type uint8

const (
	_ Type = iota
	// ExecutionStarted is the first event: Name and Version identify the
	// workflow, Payload is the input, Queue the queue.
	ExecutionStarted
	// ExecutionCompleted ends the execution with Payload as output.
	ExecutionCompleted
	// ExecutionFailed ends the execution with the error.
	ExecutionFailed
	// ExecutionContinuedAsNew ends the execution; Payload is the input of
	// the next one.
	ExecutionContinuedAsNew
	// ExecutionCanceled ends the execution after a cancel request.
	ExecutionCanceled
	// ExecutionTerminated ends the execution by force (no workflow code
	// runs, no compensation).
	ExecutionTerminated
	// ExecutionCancelRequested asks the workflow to cancel (its root context
	// is canceled during replay).
	ExecutionCancelRequested

	// TaskStarted opens a workflow task. At is the database time the task
	// started, which is what flows.Now returns during this task.
	TaskStarted

	// ActivityScheduled records an activity call: Name, Payload (input),
	// ArgHash, Queue. Retry and timeout policies are not recorded: the
	// engine looks them up by Name in the registry of the binary that runs
	// the attempt.
	ActivityScheduled
	// ActivityCompleted carries the output (Payload), CompData and Attempt.
	ActivityCompleted
	// ActivityFailed carries Err, ErrKinds, Uncertain and Attempt.
	ActivityFailed
	// ActivityCancelRequested records that the workflow canceled the activity's context.
	ActivityCancelRequested

	// TimerScheduled records a timer: At (database time it fires) and Name.
	TimerScheduled
	// TimerFired reports that the timer of ScheduleID fired.
	TimerFired
	// TimerCanceled records that the workflow canceled the timer of ScheduleID.
	TimerCanceled

	// SignalReceived delivers a signal: Name and Payload.
	SignalReceived

	// SideEffectResult records a value computed by the workflow (Payload),
	// such as flows.Rand.
	SideEffectResult

	// ChildScheduled records a child workflow start: Name and Version
	// (workflow), Ref (child instance id), Queue, Payload, ArgHash.
	ChildScheduled
	// ChildCompleted carries the output of a child (Payload).
	ChildCompleted
	// ChildFailed carries the error of a child (Err, ErrKinds).
	ChildFailed
	// ChildCancelRequested records that the parent canceled the child's context.
	ChildCancelRequested

	// CompensationScheduled starts the compensation of the activity of
	// ScheduleID: Name, Queue, Payload (its compensation data) and
	// Uncertain (the activity may not have had its effect). Compensation
	// events follow the workflow's end (ExecutionFailed or
	// ExecutionCanceled): they are written by the engine from the history,
	// not by workflow code.
	CompensationScheduled
	// CompensationCompleted reports that the compensation of ScheduleID
	// ran (or was not needed: the activity has no compensator).
	CompensationCompleted
	// CompensationFailed reports that the compensation of ScheduleID failed
	// for good (retries exhausted): the execution is parked.
	CompensationFailed
	// CompensationRetryRequested resumes a parked compensation (an operator
	// fixed the cause).
	CompensationRetryRequested

	// LockRequested asks for the business lock Name: Payload is its TTL in
	// microseconds (decimal), ArgHash its hash. The store answers with a
	// pending LockAcquired or LockHeld (Err: the holder's instance id).
	LockRequested
	// LockAcquired reports that the lock of ScheduleID is held.
	LockAcquired
	// LockHeld reports that another execution holds the lock of ScheduleID.
	LockHeld
	// LockLost reports that another execution took the lock Name (acquired
	// by ScheduleID) after it expired: the workflow fails with it.
	LockLost

	// ExecutionCompensationRequested asks a completed execution (a child
	// whose parent compensates) to compensate its activities and children.
	ExecutionCompensationRequested

	typeCount // keep last
)

var typeNames = [typeCount]string{
	ExecutionStarted:               "ExecutionStarted",
	ExecutionCompleted:             "ExecutionCompleted",
	ExecutionFailed:                "ExecutionFailed",
	ExecutionContinuedAsNew:        "ExecutionContinuedAsNew",
	ExecutionCanceled:              "ExecutionCanceled",
	ExecutionTerminated:            "ExecutionTerminated",
	ExecutionCancelRequested:       "ExecutionCancelRequested",
	TaskStarted:                    "TaskStarted",
	ActivityScheduled:              "ActivityScheduled",
	ActivityCompleted:              "ActivityCompleted",
	ActivityFailed:                 "ActivityFailed",
	ActivityCancelRequested:        "ActivityCancelRequested",
	TimerScheduled:                 "TimerScheduled",
	TimerFired:                     "TimerFired",
	TimerCanceled:                  "TimerCanceled",
	SignalReceived:                 "SignalReceived",
	SideEffectResult:               "SideEffectResult",
	ChildScheduled:                 "ChildScheduled",
	ChildCompleted:                 "ChildCompleted",
	ChildFailed:                    "ChildFailed",
	ChildCancelRequested:           "ChildCancelRequested",
	CompensationScheduled:          "CompensationScheduled",
	CompensationCompleted:          "CompensationCompleted",
	CompensationFailed:             "CompensationFailed",
	CompensationRetryRequested:     "CompensationRetryRequested",
	LockRequested:                  "LockRequested",
	LockAcquired:                   "LockAcquired",
	LockHeld:                       "LockHeld",
	LockLost:                       "LockLost",
	ExecutionCompensationRequested: "ExecutionCompensationRequested",
}

func (t Type) String() string {
	if t > 0 && t < typeCount {
		return typeNames[t]
	}
	return "Type(" + strconv.Itoa(int(t)) + ")"
}

// Valid reports whether t is a known type (a newer binary may have written
// an unknown one: the executor hands such an instance back).
func (t Type) Valid() bool { return t > 0 && t < typeCount }

// IsCommand reports whether events of this type are produced by a workflow
// command and must match it during replay (determinism check).
func (t Type) IsCommand() bool {
	switch t {
	case ActivityScheduled, ActivityCancelRequested, TimerScheduled, TimerCanceled,
		SideEffectResult, ChildScheduled, ChildCancelRequested, LockRequested:
		return true
	}
	return false
}

// IsTerminal reports whether the type ends an execution.
func (t Type) IsTerminal() bool {
	switch t {
	case ExecutionCompleted, ExecutionFailed, ExecutionContinuedAsNew, ExecutionCanceled, ExecutionTerminated:
		return true
	}
	return false
}

// Event is one entry of an execution's history (or a pending event waiting
// to be delivered to the next workflow task). Fields unused by a type are
// zero. ErrKinds is a comma-separated list, so kind names must not contain
// commas. TODO(T4.4): enforce this where error kinds are registered.
type Event struct {
	// Seq is the 1-based position in the history, assigned when the event is
	// committed. 0 for pending events.
	Seq int64
	// Type is the kind of event.
	Type Type
	// ScheduleID ties an event to the command that produced it (an
	// activity, timer, child or side effect): the scheduling event and its
	// outcome share it. Assigned by the workflow in command order.
	ScheduleID int32
	// Attempt is the activity attempt that produced the outcome (1-based).
	Attempt int32
	// Name is the activity, workflow, timer or signal name.
	Name string
	// Version is the workflow version (ExecutionStarted, ChildScheduled).
	Version int32
	// Ref is the child instance id (ChildScheduled).
	Ref string
	// Queue is the queue of an activity, a child, or the execution.
	Queue string
	// ArgHash is the FNV-1a hash of the scheduled input payload, compared
	// during replay (GW-11: go-workflows did not compare arguments).
	ArgHash uint64
	// Payload is the encoded input, output, signal value or side effect.
	Payload []byte
	// CompData is the encoded compensation data of a completed activity.
	CompData []byte
	// Err is the error message (storage-safe text).
	Err string
	// ErrKinds are the sentinel error kinds of Err, comma separated, so that
	// errors.Is keeps working after replay.
	ErrKinds string
	// Uncertain marks an activity outcome where the side effect may or may
	// not have happened (crash, timeout, panic, unstorable output); its
	// compensator still runs, told so by CompensationUncertain.
	Uncertain bool
	// Permanent marks an error that must not be retried.
	Permanent bool
	// At is a database time with microsecond precision (Postgres
	// timestamptz): when a timer fires, or when a task started. For a
	// pending TimerFired it is also the visibility time of the event.
	At time.Time
}

// HashArgs returns the FNV-1a 64 hash of p (no allocation).
func HashArgs(p []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range p {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}
