package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// attemptLog records the first attempt outcome an observer sees.
type attemptLog struct {
	got     atomic.Bool
	outcome atomic.Value // string
	err     atomic.Value // error box
}

type errBox struct{ err error }

func (l *attemptLog) ActivityAttempt(_ *store.ActivityTask, outcome string, _ time.Duration, err error) {
	if l.got.CompareAndSwap(false, true) {
		l.outcome.Store(outcome)
		l.err.Store(errBox{err})
	}
}

func (*attemptLog) Ended(store.EndInfo) {}

// T11.3 review: an exhausted task claimed by an engine that does not
// define the activity (a mixed fleet) is not handed back as an ordinary
// task: that would clear its lost state, and the next engine would run it
// again past its limit. Its lease lapses instead, so the engine that
// defines it finds it still lost, and records the exhausted failure
// without running it.
func TestExhaustedTaskSurvivesHandBack(t *testing.T) {
	ctx := context.Background()
	st := mem.New(nil)
	create(t, st, "x", "w", "")
	// The workflow schedules the activity (one attempt allowed); its first
	// attempt is lost: its holder dies.
	tasks, err := st.ClaimWorkflowTasks(ctx, "setup", []string{q}, 1, time.Second)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %v %v", tasks, err)
	}
	task := tasks[0]
	start := task.Pending[0].Event
	start.Seq = 1
	c := &store.Completion{
		History:    []history.Event{start, {Seq: 2, Type: history.ActivityScheduled, ScheduleID: 1, Name: "work", Queue: q}},
		Consumed:   []int64{task.Pending[0].ID},
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "work", Queue: q, MaxAttempts: 1}},
	}
	if _, err := st.CompleteWorkflowTask(ctx, task.Lease, c); err != nil {
		t.Fatal(err)
	}
	if acts, err := st.ClaimActivityTasks(ctx, "dead", []string{q}, 1, 100*time.Millisecond); err != nil || len(acts) != 1 {
		t.Fatalf("claim activity: %v %v", acts, err)
	}
	time.Sleep(150 * time.Millisecond)

	// A does not define the activity (nor the workflow).
	startWorker(t, st, newRegistry(), worker.Options{Owner: "a", LeaseTTL: 300 * time.Millisecond, HandBack: 20 * time.Millisecond})
	time.Sleep(500 * time.Millisecond) // A claims it, more than once

	var calls atomic.Int32
	reg := newRegistry()
	reg.addActivity("work", &worker.Activity{Retry: worker.RetryPolicy{Attempts: 1}, Run: func(context.Context, *store.ActivityTask) ([]byte, []byte, error) {
		calls.Add(1)
		return []byte("done"), nil, nil
	}})
	log := &attemptLog{}
	startWorker(t, st, reg, worker.Options{Owner: "b", LeaseTTL: 300 * time.Millisecond, HandBack: 20 * time.Millisecond, Observer: log})
	deadline := time.Now().Add(10 * time.Second)
	for !log.got.Load() {
		if time.Now().After(deadline) {
			t.Fatal("B recorded no attempt")
		}
		time.Sleep(10 * time.Millisecond)
	}
	err = log.err.Load().(errBox).err
	if out := log.outcome.Load().(string); out != "failed" || !errors.Is(err, worker.ErrAttemptsExhausted) || calls.Load() != 0 {
		t.Fatalf("B: %s %v, ran %d times; want failed with ErrAttemptsExhausted, never run", out, err, calls.Load())
	}
}
