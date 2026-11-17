package flows

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/text"
	"github.com/imdat99/go-flows/internal/worker"
)

var (
	// ErrInvalidArgument reports an empty, too long (over 1024 bytes),
	// invalid UTF-8 or NUL-containing name, instance id or signal name.
	ErrInvalidArgument = errors.New("flows: invalid argument")
	// ErrNotFound reports an unknown instance id.
	ErrNotFound = store.ErrNotFound
	// ErrNotActive reports an instance whose latest execution ended.
	ErrNotActive = store.ErrNotActive
	// ErrLeaseLost is matched by the error a Tracer's end gets for an
	// activity attempt or workflow task whose lease was lost: another
	// engine took its task and runs it again (its outcome is not recorded).
	ErrLeaseLost = store.ErrLeaseLost
	// ErrRerun is matched by the error a Tracer's start end gets when the
	// completion that would have stored the start (a child, a
	// continue-as-new) was refused because signals or cancel requests
	// arrived meanwhile: not a failure, the task runs again with them and
	// makes the start again (created is false).
	ErrRerun = store.ErrPendingEvents
	// ErrInstanceConflict reports an instance id already used by another
	// workflow: an instance id is an idempotency key.
	ErrInstanceConflict = store.ErrInstanceConflict
	// ErrNonDeterministic is the reason of an execution parked as
	// needs_attention: its replay diverged from its history (the deployed
	// code no longer matches it). Engine.Resume resumes it once fixed code
	// is deployed.
	ErrNonDeterministic = executor.ErrNonDeterministic
	// ErrStuck is the reason of an execution parked as needs_attention
	// because its workflow code ran without yielding to the engine for
	// longer than Options.StuckTimeout, twice at the same point of its
	// history (a blocking call or a busy loop: do such work in activities;
	// once is handed back, as a stalled process).
	// Its goroutine cannot be stopped and stays until the process exits.
	// Engine.Resume resumes it once fixed code is deployed.
	ErrStuck = coro.ErrStuck
	// ErrCrashLoop is the reason of an execution parked as needs_attention
	// because its workflow task was claimed more than
	// Options.MaxTaskClaims times without ever completing: the
	// processes running it crashed (out of memory, os.Exit, a fatal runtime
	// error), or lost their lease, each time. Engine.Resume resumes it once
	// the cause is fixed.
	ErrCrashLoop = worker.ErrCrashLoop
	// ErrAttemptsExhausted is the error of an activity (or compensator)
	// whose attempts reached its retry policy's limit while the last one
	// was lost: its process crashed (out of memory, os.Exit, a fatal
	// runtime error) or lost its lease without recording a result. It is
	// not run again: it fails permanently, uncertain (it may have had its
	// effect, so it is compensated), as any final failure. A lost attempt
	// may still be running (a partitioned or paused process): key its
	// effect (idem) and fence the key in the compensator (idem.Abort). The
	// limit is the retry policy of the binary that scheduled the activity;
	// none when that binary does not define it.
	ErrAttemptsExhausted = worker.ErrAttemptsExhausted
	// ErrCanceled is the error of an activity, timer, child or workflow whose
	// context was canceled.
	ErrCanceled = executor.ErrCanceled
	// ErrActivityCanceled is the cause of an activity's context canceled
	// because the workflow canceled the activity.
	ErrActivityCanceled = worker.ErrActivityCanceled
	// ErrPanic wraps a panic recovered from an activity or workflow; it is
	// permanent, and a panicking activity is uncertain.
	ErrPanic = worker.ErrPanic
	// ErrTimeout is the error of an activity attempt that exceeded its
	// timeout (uncertain: it may have had its effect).
	ErrTimeout = errors.New("flows: activity timed out")
	// ErrAwaitTimeout reports a signal not received in time (Await).
	ErrAwaitTimeout = errors.New("flows: signal not received in time")
	// ErrTerminated is the error of a terminated workflow.
	ErrTerminated = errors.New("flows: terminated")
	// ErrInstanceExists is the error of a child workflow whose instance id
	// was already in use.
	ErrInstanceExists = errors.New("flows: instance id already in use")
	// ErrNotInWorkflow reports a workflow API called outside a workflow.
	ErrNotInWorkflow = errors.New("flows: called outside a workflow")
	// ErrCompensated matches the error of a workflow that failed and whose
	// completed activities were compensated (status compensated).
	ErrCompensated = errors.New("flows: compensated")
	// ErrCompensationFailed is the reason of an execution parked as
	// needs_attention because a compensation failed after its retries;
	// Engine.RetryCompensation resumes it.
	ErrCompensationFailed = errors.New("flows: compensation failed")
)

// permanentError marks an error as not retryable.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as permanent: an activity failing with it is not
// retried. Wrapping it further (%w) keeps it permanent (errors.As).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or an error it wraps, is permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Error kinds: sentinel errors recorded by name with an error, so that
// errors.Is keeps matching after the error was stored and replayed.
var kinds = struct {
	sync.RWMutex
	list []errorKind
}{list: []errorKind{
	{"panic", ErrPanic},
	{"timeout", ErrTimeout},
	{"canceled", ErrCanceled},
	{"await_timeout", ErrAwaitTimeout},
	{store.KindNonDeterministic, ErrNonDeterministic},
	{store.KindStuck, ErrStuck},
	{store.KindTerminated, ErrTerminated},
	{store.KindInstanceExists, ErrInstanceExists},
	{"activity_canceled", ErrActivityCanceled},
	{store.KindCompensated, ErrCompensated},
	{store.KindCompensationFailed, ErrCompensationFailed},
	{"invalid_argument", ErrInvalidArgument},
	{"lock_held", ErrLockHeld},
	{"lock_lost", ErrLockLost},
	{"attempts_exhausted", ErrAttemptsExhausted},
	{store.KindCrashLoop, ErrCrashLoop},
}}

type errorKind struct {
	name     string
	sentinel error
}

const kindPermanent = "permanent"

// RegisterErrorKind makes errors.Is(err, sentinel) keep working for errors
// recorded in a workflow's history (an activity's error, as seen by the
// workflow on every replay; a workflow's error, as returned by Wait). name
// is stored: it must be stable, valid text (see Engine.Signal) without
// commas or spaces, and not "permanent". Register at init, in every binary
// that runs the workflows.
func RegisterErrorKind(name string, sentinel error) {
	if err := text.CheckName(ErrInvalidArgument, "error kind", name); err != nil {
		panic(err)
	}
	if strings.ContainsAny(name, ", ") || name == kindPermanent || sentinel == nil {
		panic(fmt.Sprintf("flows: invalid error kind %q", name))
	}
	if !reflect.TypeOf(sentinel).Comparable() { // errors.Is would panic comparing it
		panic(fmt.Sprintf("flows: error kind %q: sentinel of type %T is not comparable", name, sentinel))
	}
	kinds.Lock()
	defer kinds.Unlock()
	for _, k := range kinds.list {
		if k.name == name {
			panic(fmt.Sprintf("flows: error kind %q registered twice", name))
		}
	}
	kinds.list = append(kinds.list, errorKind{name, sentinel})
}

// errorKinds lists, comma separated, every kind err matches.
func errorKinds(err error) string {
	var out []string
	var pe *coro.PanicError
	if errors.As(err, &pe) { // a panic in workflow code
		out = append(out, "panic")
	}
	if IsPermanent(err) || pe != nil {
		out = append(out, kindPermanent)
	}
	if re, ok := errors.AsType[*RecordedError](err); ok {
		for _, k := range re.kinds {
			if k != kindPermanent {
				out = append(out, k)
			}
		}
	}
	for _, k := range kindList() { // not under the lock: errors.Is calls RecordedError.Is, which takes it
		if errors.Is(err, k.sentinel) && !contains(out, k.name) {
			out = append(out, k.name)
		}
	}
	return strings.Join(out, ",")
}

func contains(s []string, v string) bool { return slices.Contains(s, v) }

// kindList returns the registered kinds. The list only grows (appends
// never change its elements), so the slice can be read without the lock.
func kindList() []errorKind {
	kinds.RLock()
	defer kinds.RUnlock()
	return kinds.list
}

// maxErrorText bounds stored error text (errors are for people; histories
// stay small).
const maxErrorText = 4096

// RecordedError is an error as recorded in a history (an activity's or a
// child's failure seen by a workflow, a workflow's failure seen by Wait):
// its message and its kinds. errors.Is matches the sentinels of its kinds
// (RegisterErrorKind) and IsPermanent its permanence.
type RecordedError struct {
	Message string
	// Uncertain reports an activity that may have had its effect although
	// it failed (crash, timeout, panic, cancel).
	Uncertain bool
	kinds     []string
}

func (e *RecordedError) Error() string { return e.Message }

// Is matches the sentinels of the recorded kinds.
func (e *RecordedError) Is(target error) bool {
	for _, k := range kindList() {
		if k.sentinel == target && contains(e.kinds, k.name) {
			return true
		}
	}
	return false
}

// As makes IsPermanent work on recorded permanent errors.
func (e *RecordedError) As(target any) bool {
	if p, ok := target.(**permanentError); ok && contains(e.kinds, kindPermanent) {
		*p = &permanentError{err: e}
		return true
	}
	return false
}

func recorded(msg, kindList string, uncertain bool) *RecordedError {
	re := &RecordedError{Message: msg, Uncertain: uncertain}
	if kindList != "" {
		re.kinds = strings.Split(kindList, ",")
	}
	return re
}

// fromExecutor converts an executor outcome error into the public form.
func fromExecutor(err error) error {
	if re, ok := errors.AsType[*executor.RecordedError](err); ok {
		r := recorded(re.Message, strings.Join(re.Kinds, ","), re.Uncertain)
		if re.Permanent && !contains(r.kinds, kindPermanent) {
			r.kinds = append(r.kinds, kindPermanent)
		}
		return r
	}
	return err
}

// errorCodec records errors for the worker and the executor.
type errorCodec struct{}

func (errorCodec) Text(err error) string    { return text.Safe(err.Error(), maxErrorText) }
func (errorCodec) Kinds(err error) string   { return errorKinds(err) }
func (errorCodec) Permanent(err error) bool { return IsPermanent(err) }

// RunError is the error of a workflow run that did not complete: it failed,
// was compensated, canceled, terminated, or needs attention.
type RunError struct {
	InstanceID string
	Status     Status
	Cause      *RecordedError
}

func (e *RunError) Error() string {
	return fmt.Sprintf("flows: %s %s: %s", e.InstanceID, e.Status, e.Cause.Message)
}

// Unwrap returns the recorded cause (errors.Is matches its kinds).
func (e *RunError) Unwrap() error { return e.Cause }
