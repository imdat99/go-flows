package storetest

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// testWatch: a store that implements store.Watcher sends a hint for new
// executions, for signals to an idle execution and for unclaimed activity
// tasks (GW-8: pollers can back off between hints).
func testWatch(t *testing.T, h Harness) {
	s := h.New(t)
	w, ok := s.(store.Watcher)
	if !ok {
		t.Skip("store does not implement store.Watcher")
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type hint struct {
		kind  store.WorkKind
		queue string
	}
	hints := make(chan hint, 64)
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = w.Watch(wctx, func(k store.WorkKind, q string) {
			select {
			case hints <- hint{k, q}:
			default:
			}
		})
	})
	defer wg.Wait()
	defer cancel()
	drain := func() {
		for {
			select {
			case <-hints:
			default:
				return
			}
		}
	}
	expect := func(what string, want hint) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case got := <-hints:
				if got == want {
					return
				}
			case <-deadline:
				t.Fatalf("no %s hint %+v", what, want)
			}
		}
	}
	// Once listening, the watch wakes both kinds for every queue (work made
	// ready before it listened got no hint): T8.4.
	expect("listening", hint{store.WorkflowWork, ""})
	expect("listening", hint{store.ActivityWork, ""})
	// Until the watch listens, hints are lost: create executions until one
	// is hinted.
	for i := 0; ; i++ {
		mustCreate(t, s, "warmup"+strconv.Itoa(i))
		select {
		case <-hints:
		case <-time.After(200 * time.Millisecond):
			if i < 25 {
				continue
			}
			t.Fatal("the watch never delivered a hint")
		}
		break
	}
	drain()
	mustCreate(t, s, "i1")
	expect("create", hint{store.WorkflowWork, queue})
	var task store.WorkflowTask
	for _, tk := range claim(t, s, owner, h.TTL) {
		if tk.Execution.InstanceID == "i1" {
			task = tk
		}
	}
	drain()
	started(t, s, task, &store.Completion{
		History:    []history.Event{{Type: history.ActivityScheduled, ScheduleID: 1, Name: "a"}},
		Activities: []store.NewActivityTask{{ScheduleID: 1, Name: "a", Queue: "acts"}},
	})
	expect("activity", hint{store.ActivityWork, "acts"})
	drain()
	if err := s.Signal(ctx, "i1", "x", nil); err != nil {
		t.Fatal(err)
	}
	expect("signal", hint{store.WorkflowWork, queue})

	// The eager path sends no hint: NOTIFY costs a global lock at commit.
	again := claimOne(t, s, owner, h.TTL)
	drain()
	leases, err := s.CompleteWorkflowTask(ctx, again.Lease, &store.Completion{
		History:    []history.Event{{Seq: again.LastSeq + 1, Type: history.ActivityScheduled, ScheduleID: 2, Name: "a"}},
		Consumed:   pendingIDs(again),
		Activities: []store.NewActivityTask{{ScheduleID: 2, Name: "a", Queue: queue, ClaimFor: h.TTL}},
		KeepFor:    h.TTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteActivityTask(ctx, leases[0], store.ActivityResult{
		Event: history.Event{Type: history.ActivityCompleted}, Resume: &again.Lease, ResumeFor: h.TTL,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-hints:
		t.Fatalf("eager path sent a hint: %+v", got)
	case <-time.After(300 * time.Millisecond):
	}
}
