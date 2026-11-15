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
