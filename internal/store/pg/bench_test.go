package pg_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// BenchmarkCreateExecution measures Workflow.Start's store side, one new
// instance id per op, with tombstones of purged ids off (the default: the
// statement of earlier releases) and kept (T12.3: one more index probe).
func BenchmarkCreateExecution(b *testing.B) {
	for _, keep := range []time.Duration{0, time.Hour} {
		name := "off"
		if keep > 0 {
			name = "kept"
		}
		b.Run(name, func(b *testing.B) {
			s := newStoreB(b)
			s.KeepPurged(keep)
			ctx := context.Background()
			ids := make([]string, b.N)
			for i := range ids {
				ids[i] = "bench-" + strconv.Itoa(i)
			}
			input := []byte(`{"order_id":"9f1c2a4e-5b6d-4c3e-8f7a-1b2c3d4e5f60","amount":12999}`)
			b.ResetTimer()
			for i := range b.N {
				if _, created, err := s.CreateExecution(ctx, store.NewExecution{
					InstanceID: ids[i], Workflow: "w", Version: 1, Queue: "q", Input: input,
				}); err != nil || !created {
					b.Fatal(created, err)
				}
			}
		})
	}
}

// BenchmarkEagerActivity measures the store side of the eager loop, one
// activity per op: complete the workflow task (history + eager activity
// claim + keep the lease), then complete the activity and resume the
// workflow, both one round trip. Target: 2 round trips per activity.
func BenchmarkEagerActivity(b *testing.B) {
	s := newStoreB(b)
	ctx := context.Background()
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "bench", Workflow: "w", Queue: "q"}); err != nil {
		b.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		b.Fatal(err)
	}
	task := tasks[0]
	input := []byte(`{"order_id":"9f1c2a4e-5b6d-4c3e-8f7a-1b2c3d4e5f60","amount":12999}`)
	output := []byte(`{"charge_id":"ch_123"}`)
	seq := task.LastSeq
	consumed := []int64{task.Pending[0].ID}
	events := make([]history.Event, 0, 4)
	acts := make([]store.NewActivityTask, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		sched := int32(i + 1)
		events = events[:0]
		if seq == 0 {
			seq++
			events = append(events, history.Event{Seq: seq, Type: history.ExecutionStarted})
		}
		for _, p := range task.Pending {
			if p.Event.Type == history.ActivityCompleted {
				seq++
				p.Event.Seq = seq
				events = append(events, p.Event)
			}
		}
		seq++
		events = append(events, history.Event{
			Seq: seq, Type: history.ActivityScheduled, ScheduleID: sched, Name: "charge",
			Payload: input, ArgHash: history.HashArgs(input),
		})
		acts[0] = store.NewActivityTask{ScheduleID: sched, Name: "charge", Queue: "q", Input: input, ClaimFor: time.Minute}
		c := store.Completion{History: events, Consumed: consumed, Activities: acts, KeepFor: time.Minute}
		leases, err := s.CompleteWorkflowTask(ctx, task.Lease, &c)
		if err != nil {
			b.Fatal(err)
		}
		next, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{
			Event:  history.Event{Type: history.ActivityCompleted, Payload: output},
			Resume: &task.Lease, ResumeFor: time.Minute,
		})
		if err != nil || next == nil {
			b.Fatal(err, next)
		}
		task = *next
		consumed = consumed[:0]
		for _, p := range task.Pending {
			consumed = append(consumed, p.ID)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "µs/activity")
}

// BenchmarkLockStep measures a workflow step that takes a business lock
// and runs on with its answer under the kept lease (T9.7): the completion
// (history + lock request) returns the next task with the answer pending,
// one round trip (was a completion, then ResumeWorkflowTask: 2 round trips,
// 77 allocs, 7.4 KiB per op).
func BenchmarkLockStep(b *testing.B) {
	s := newStoreB(b)
	ctx := context.Background()
	if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: "bench", Workflow: "w", Queue: "q"}); err != nil {
		b.Fatal(err)
	}
	tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		b.Fatal(err)
	}
	task := tasks[0]
	seq := task.LastSeq
	events := make([]history.Event, 0, 4)
	consumed := make([]int64, 0, 2)
	locks := make([]store.LockRequest, 1)
	var next store.WorkflowTask
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		sched := int32(i + 1)
		events, consumed = events[:0], consumed[:0]
		for _, p := range task.Pending {
			seq++
			p.Event.Seq = seq
			events = append(events, p.Event)
			consumed = append(consumed, p.ID)
		}
		seq++
		events = append(events, history.Event{Seq: seq, Type: history.LockRequested, ScheduleID: sched, Name: "k"})
		locks[0] = store.LockRequest{ScheduleID: sched, Key: "k", TTL: time.Minute}
		c := store.Completion{History: events, Consumed: consumed, Locks: locks, KeepFor: time.Minute, Resume: &next}
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &c); err != nil || !c.Resumed {
			b.Fatal(err, errors.New("not resumed"))
		}
		task, next = next, task // two buffers in turn (task's events are still read above)
		if len(task.Pending) != 1 || task.Pending[0].Event.Type != history.LockAcquired {
			b.Fatalf("answer %+v", task.Pending)
		}
	}
}
