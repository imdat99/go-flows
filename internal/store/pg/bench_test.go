package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

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
