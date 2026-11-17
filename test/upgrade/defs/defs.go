// Package defs holds the workflows and activities of the upgrade test
// (test/upgrade): the same source is built against the release the test
// upgrades from (the old driver, olddriver) and against HEAD (the test
// itself), so both engines run the same definitions under the same names
// and versions. It must only use API present in both.
package defs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
)

// Phases: which binary runs the activities.
const (
	PhaseOld = "old"
	PhaseNew = "new"
)

// LockKey is the business lock held across the upgrade.
const LockKey = "up-res"

// TimerSleep is the durable timer pending across the upgrade: long enough
// that the old engine stops before it fires.
const TimerSleep = 20 * time.Second

// Schema creates the tables the activities record their side effects in:
// every attempt (upgrade_attempt) and, once per key, the effect itself
// (upgrade_effect, keyed by the activity key and whether it is the
// compensation).
const Schema = `
CREATE TABLE upgrade_attempt (
    id         bigserial PRIMARY KEY,
    tag        text    NOT NULL,
    key        text    NOT NULL,
    compensate boolean NOT NULL,
    phase      text    NOT NULL,
    ok         boolean NOT NULL,
    applied    boolean NOT NULL
);
CREATE TABLE upgrade_effect (
    key        text    NOT NULL,
    compensate boolean NOT NULL,
    tag        text    NOT NULL,
    PRIMARY KEY (key, compensate)
);`

var (
	db    *pgxpool.Pool
	phase string
)

// Setup gives the activities their database and phase; call it before the
// engine starts.
func Setup(pool *pgxpool.Pool, p string) {
	db = pool
	phase = p
}

// Options are the engine options of both phases.
func Options(instanceID string, logger *slog.Logger) flows.Options {
	return flows.Options{
		InstanceID: instanceID,
		LeaseTTL:   5 * time.Second,
		PollMax:    200 * time.Millisecond,
		Logger:     logger,
	}
}

// Step is the input of the step activity.
type Step struct {
	Tag string `json:"tag"`
	// FailOld fails every attempt run by the old binary (retried after the
	// upgrade).
	FailOld bool `json:"fail_old,omitempty"`
	// FailFirst fails the first attempts.
	FailFirst int `json:"fail_first,omitempty"`
	// CompFailOld fails every compensation attempt run by the old binary.
	CompFailOld bool `json:"comp_fail_old,omitempty"`
}

// Undo is the compensation data of the step activity.
type Undo struct {
	Tag     string `json:"tag"`
	FailOld bool   `json:"fail_old,omitempty"`
}

// ErrDeclined fails the saga workflows.
var ErrDeclined = errors.New("declined")

var errPlanned = errors.New("planned failure")

func retry() flows.RetryPolicy { return flows.Exponential(10000, 200*time.Millisecond, time.Second) }

// StepAct records its attempt and, once per key, its effect.
var StepAct = flows.DefineActivity("up.step",
	func(ctx context.Context, k flows.Key, in Step) (string, Undo, error) {
		fail := in.FailOld && phase == PhaseOld
		if !fail && in.FailFirst > 0 {
			var n int
			if err := db.QueryRow(ctx, `SELECT count(*) FROM upgrade_attempt WHERE tag = $1 AND NOT compensate`,
				in.Tag).Scan(&n); err != nil {
				return "", Undo{}, err
			}
			fail = n < in.FailFirst
		}
		if err := record(ctx, in.Tag, k, false, !fail); err != nil {
			return "", Undo{}, err
		}
		if fail {
			return "", Undo{}, errPlanned
		}
		return "done:" + in.Tag, Undo{Tag: in.Tag, FailOld: in.CompFailOld}, nil
	},
	func(ctx context.Context, k flows.Key, u Undo) error {
		if u.Tag == "" {
			return flows.Permanent(errors.New("compensation data lost"))
		}
		fail := u.FailOld && phase == PhaseOld
		if err := record(ctx, u.Tag, k, true, !fail); err != nil {
			return err
		}
		if fail {
			return errPlanned
		}
		return nil
	},
	flows.WithRetry(retry()), flows.WithCompensateRetry(retry()))

// record writes an attempt and, when ok, the effect (once per key).
func record(ctx context.Context, tag string, k flows.Key, comp, ok bool) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		applied := false
		if ok {
			ct, err := tx.Exec(ctx, `INSERT INTO upgrade_effect (key, compensate, tag) VALUES ($1, $2, $3)
				ON CONFLICT DO NOTHING`, string(k), comp, tag)
			if err != nil {
				return err
			}
			applied = ct.RowsAffected() == 1
		}
		_, err := tx.Exec(ctx, `INSERT INTO upgrade_attempt (tag, key, compensate, phase, ok, applied)
			VALUES ($1, $2, $3, $4, $5, $6)`, tag, string(k), comp, phase, ok, applied)
		return err
	})
}

func run(c flows.Ctx, tag string) (string, error) { return StepAct.Run(c, Step{Tag: tag}) }

// TimerIn is the input of Timer.
type TimerIn struct {
	Tag   string        `json:"tag"`
	Sleep time.Duration `json:"sleep"`
}

// CanIn is the input of Can.
type CanIn struct {
	Tag string `json:"tag"`
	Gen int    `json:"gen"`
}

// The workflows. Each waits at a point the upgrade happens at.
var (
	// Retry runs an activity whose attempts fail until the upgrade.
	Retry = flows.DefineWorkflow("up.retry", 1, func(c flows.Ctx, in Step) (string, error) {
		return StepAct.Run(c, in)
	})
	// Timer sleeps across the upgrade.
	Timer = flows.DefineWorkflow("up.timer", 1, func(c flows.Ctx, in TimerIn) (string, error) {
		if _, err := run(c, in.Tag+"-a"); err != nil {
			return "", err
		}
		if err := flows.Sleep(c, in.Sleep); err != nil {
			return "", err
		}
		return run(c, in.Tag+"-b")
	})
	// Signal waits for the signal "go" across the upgrade; its payload is
	// the tag of the next step.
	Signal = flows.DefineWorkflow("up.signal", 1, func(c flows.Ctx, tag string) (string, error) {
		if _, err := run(c, tag+"-a"); err != nil {
			return "", err
		}
		next, err := flows.Await[string](c, "go", 0)
		if err != nil {
			return "", err
		}
		return run(c, next)
	})
	// Locked holds LockKey across the upgrade, until the signal "release".
	Locked = flows.DefineWorkflow("up.lock", 1, func(c flows.Ctx, tag string) (string, error) {
		if err := flows.Lock(c, LockKey, 10*time.Minute); err != nil {
			return "", err
		}
		if _, err := run(c, tag+"-a"); err != nil {
			return "", err
		}
		if _, err := flows.Await[string](c, "release", 0); err != nil {
			return "", err
		}
		return run(c, tag+"-b")
	})
	// LockProbe reports whether LockKey is held by another execution.
	LockProbe = flows.DefineWorkflow("up.lockprobe", 1, func(c flows.Ctx, tag string) (string, error) {
		err := flows.Lock(c, LockKey, time.Minute)
		if errors.Is(err, flows.ErrLockHeld) {
			return "held", nil
		}
		if err != nil {
			return "", err
		}
		if _, err := run(c, tag); err != nil {
			return "", err
		}
		return "free", nil
	})
	// Saga runs two steps, waits for "proceed", then fails: the steps are
	// compensated after the upgrade.
	Saga = flows.DefineWorkflow("up.saga", 1, func(c flows.Ctx, tag string) (string, error) {
		if _, err := run(c, tag+"-debit"); err != nil {
			return "", err
		}
		if _, err := run(c, tag+"-credit"); err != nil {
			return "", err
		}
		if _, err := flows.Await[string](c, "proceed", 0); err != nil {
			return "", err
		}
		return "", ErrDeclined
	})
	// SagaMid fails at once; its compensator fails until the upgrade, so
	// it compensates across it.
	SagaMid = flows.DefineWorkflow("up.sagamid", 1, func(c flows.Ctx, tag string) (string, error) {
		if _, err := StepAct.Run(c, Step{Tag: tag + "-debit", CompFailOld: true}); err != nil {
			return "", err
		}
		return "", ErrDeclined
	})
	// Can is a continue-as-new chain of three executions; the second waits
	// for "go" across the upgrade.
	Can = flows.DefineWorkflow("up.can", 1, func(c flows.Ctx, in CanIn) (string, error) {
		if in.Gen == 1 {
			if _, err := flows.Await[string](c, "go", 0); err != nil {
				return "", err
			}
		}
		out, err := run(c, in.Tag+"-"+strconv.Itoa(in.Gen))
		if err != nil {
			return "", err
		}
		if in.Gen < 2 {
			return "", flows.ContinueAsNew(c, CanIn{Tag: in.Tag, Gen: in.Gen + 1})
		}
		return out, nil
	})
	// Child waits for "child-go" across the upgrade.
	Child = flows.DefineWorkflow("up.child", 1, func(c flows.Ctx, tag string) (string, error) {
		if _, err := run(c, tag+"-a"); err != nil {
			return "", err
		}
		if _, err := flows.Await[string](c, "child-go", 0); err != nil {
			return "", err
		}
		out, err := run(c, tag+"-b")
		return "child:" + out, err
	})
	// Parent waits for Child across the upgrade.
	Parent = flows.DefineWorkflow("up.parent", 1, func(c flows.Ctx, tag string) (string, error) {
		r, err := Child.Child(c, "up-"+tag+"-c", tag+"-c").Get(c)
		if err != nil {
			return "", err
		}
		out, err := run(c, tag)
		return r + "|" + out, err
	})
	// Child2 completes at once.
	Child2 = flows.DefineWorkflow("up.child2", 1, func(c flows.Ctx, tag string) (string, error) {
		return run(c, tag+"-a")
	})
	// ParentFail runs a step and a child before the upgrade, then fails
	// after it on "fail": its step and its completed child are compensated
	// by the new engine.
	ParentFail = flows.DefineWorkflow("up.parentfail", 1, func(c flows.Ctx, tag string) (string, error) {
		if _, err := run(c, tag+"-p"); err != nil {
			return "", err
		}
		if _, err := Child2.Child(c, "up-"+tag+"-c", tag+"-c").Get(c); err != nil {
			return "", err
		}
		if _, err := flows.Await[string](c, "fail", 0); err != nil {
			return "", err
		}
		return "", ErrDeclined
	})
)

// StartOld starts the executions the old engine takes to their waiting
// points.
func StartOld(ctx context.Context, e *flows.Engine) error {
	var errs []error
	add := func(_ any, err error) { errs = append(errs, err) }
	add(Retry.Start(ctx, e, "up-retry", Step{Tag: "retry", FailOld: true}))
	add(Timer.Start(ctx, e, "up-timer", TimerIn{Tag: "timer", Sleep: TimerSleep}))
	add(Signal.Start(ctx, e, "up-signal", "signal"))
	add(Locked.Start(ctx, e, "up-lock", "lock"))
	add(Saga.Start(ctx, e, "up-saga", "saga"))
	add(SagaMid.Start(ctx, e, "up-sagamid", "sagamid"))
	add(Can.Start(ctx, e, "up-can", CanIn{Tag: "can"}))
	add(Parent.Start(ctx, e, "up-parent", "parent"))
	add(ParentFail.Start(ctx, e, "up-pf", "pf"))
	return errors.Join(errs...)
}

// condition is a state an execution must be in at the upgrade.
type condition struct {
	name  string
	query string
	args  []any
}

// histCount counts the events of type t in the history of the running
// execution of the instance id $1 (as an SQL expression).
func histCount(param int, t history.Type) string {
	return `(SELECT count(*) FROM flows.history h JOIN flows.execution e ON e.id = h.execution_id
		WHERE e.instance_id = $` + strconv.Itoa(param) + ` AND e.status = 'running' AND h.type = ` +
		strconv.Itoa(int(t)) + `)`
}

func conditions() []condition {
	return []condition{
		{
			"retry: activity retrying", `SELECT (SELECT count(*) FROM upgrade_attempt WHERE tag = 'retry' AND NOT ok) >= 2
			AND EXISTS (SELECT 1 FROM flows.activity_task WHERE instance_id = $1 AND NOT compensate)`,
			[]any{"up-retry"},
		},
		{"timer: timer pending", `SELECT ` + histCount(1, history.TimerScheduled) + ` = 1 AND ` +
			histCount(1, history.TimerFired) + ` = 0`, []any{"up-timer"}},
		{
			"signal: awaiting the signal", `SELECT ` + histCount(1, history.ActivityCompleted) + ` = 1`,
			[]any{"up-signal"},
		},
		{"lock: held, awaiting the signal", `SELECT ` + histCount(1, history.ActivityCompleted) + ` = 1
			AND EXISTS (SELECT 1 FROM flows.lock l JOIN flows.execution e ON e.id = l.execution_id
				WHERE l.key = $2 AND e.instance_id = $1)`, []any{"up-lock", LockKey}},
		{
			"saga: two steps done, awaiting the signal", `SELECT ` + histCount(1, history.ActivityCompleted) + ` = 2`,
			[]any{"up-saga"},
		},
		{
			"sagamid: compensator retrying", `SELECT EXISTS (SELECT 1 FROM flows.execution
				WHERE instance_id = $1 AND status = 'compensating')
			AND (SELECT count(*) FROM upgrade_attempt WHERE tag = 'sagamid-debit' AND compensate AND NOT ok) >= 2
			AND EXISTS (SELECT 1 FROM flows.activity_task WHERE instance_id = $1 AND compensate)`,
			[]any{"up-sagamid"},
		},
		{"can: second execution awaiting the signal", `SELECT (SELECT count(*) FROM flows.execution
				WHERE instance_id = $1 AND status = 'continued') = 1
			AND ` + histCount(1, history.TaskStarted) + ` >= 1`, []any{"up-can"}},
		{"parent: child awaiting the signal", `SELECT ` + histCount(1, history.ChildScheduled) + ` = 1
			AND ` + histCount(2, history.ActivityCompleted) + ` = 1`, []any{"up-parent", "up-parent-c"}},
		{
			"pf: child completed, awaiting the signal", `SELECT ` + histCount(1, history.ChildCompleted) + ` = 1
			AND (SELECT status FROM flows.execution WHERE instance_id = $2) = 'completed'`,
			[]any{"up-pf", "up-pf-c"},
		},
		// Events delivered, not consumed by a task yet (a timer waits as an
		// event visible when it fires).
		{"no visible pending events", `SELECT NOT EXISTS (SELECT 1 FROM flows.pending_event WHERE visible_at <= now())`, nil},
	}
}

// Unready returns the conditions of the upgrade point not met yet.
func Unready(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	var out []string
	for _, c := range conditions() {
		var ok bool
		if err := pool.QueryRow(ctx, c.query, c.args...).Scan(&ok); err != nil {
			return nil, fmt.Errorf("condition %q: %w", c.name, err)
		}
		if !ok {
			out = append(out, c.name)
		}
	}
	return out, nil
}
