package executor_test

import (
	"testing"

	"github.com/imdat99/go-flows/internal/coro"
	"github.com/imdat99/go-flows/internal/executor"
	"github.com/imdat99/go-flows/internal/history"
)

// seqWorkflow runs n activities one after the other.
func seqWorkflow(n int) executor.Workflow {
	return executor.Workflow{Name: "seq", Version: 1, Run: func(ctx coro.Context, in []byte) ([]byte, error) {
		st := executor.FromContext(ctx)
		for range n {
			r, err := st.ScheduleActivity(ctx, "a", "q", in).Get(ctx)
			if err != nil {
				return nil, err
			}
			in = r.Output
		}
		return in, nil
	}}
}

// recorded builds the history of seqWorkflow(n): per activity a task with
// its schedule and its result.
func recorded(n int) []history.Event {
	in := []byte(`{"order":"9f1c2a4e","amount":12999}`)
	h := history.HashArgs(in)
	evs := []history.Event{{Type: history.TaskStarted}, {Type: history.ExecutionStarted, Name: "seq", Version: 1, Queue: "q", Payload: in}}
	for i := range n {
		evs = append(evs,
			history.Event{Type: history.ActivityScheduled, ScheduleID: int32(i + 1), Name: "a", Queue: "q", Payload: in, ArgHash: h},
			history.Event{Type: history.TaskStarted},
			history.Event{Type: history.ActivityCompleted, ScheduleID: int32(i + 1), Payload: in})
	}
	for i := range evs {
		evs[i].Seq = int64(i + 1)
	}
	return evs
}

// BenchmarkReplay: cold replay of a 100-activity history (302 events),
// including the executor's setup and teardown.
func BenchmarkReplay(b *testing.B) {
	const n = 100
	wf := seqWorkflow(n)
	hist := recorded(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ex := executor.New(wf, 1, executor.Options{})
		if err := ex.Replay(hist); err != nil {
			b.Fatal(err)
		}
		ex.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(hist)), "ns/event")
}
