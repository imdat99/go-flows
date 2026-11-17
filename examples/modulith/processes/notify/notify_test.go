package notify

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/examples/modulith/modules/notifications"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

func TestSendRetriesTheMailServer(t *testing.T) {
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
	var attempts atomic.Int32
	flowtest.OnActivity(tst, deliver, func(context.Context, flows.Key, notifications.Message) (struct{}, struct{}, error) {
		if attempts.Add(1) < 3 {
			return struct{}{}, struct{}{}, errors.New("smtp: 421 try again later")
		}
		return struct{}{}, struct{}{}, nil
	})
	start := tst.Now()
	m := notifications.Message{EventID: "e1", Template: "order_completed"}
	if _, err := flowtest.Run(tst, send, "notifications-e1", m); err != nil {
		t.Fatal(err)
	}
	// Two failed attempts, retried after 1s and 2s of virtual time.
	if attempts.Load() != 3 || tst.Now().Sub(start) < 3*time.Second {
		t.Fatalf("%d attempts in %v", attempts.Load(), tst.Now().Sub(start))
	}
}

// The consumer translates an orders event into a message, and an event
// delivered twice sends once (one workflow per event id).
func TestSubscribeTranslatesOrdersEvents(t *testing.T) {
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
	got := make(chan notifications.Message, 4)
	flowtest.OnActivity(tst, deliver, func(_ context.Context, _ flows.Key, m notifications.Message) (struct{}, struct{}, error) {
		got <- m
		return struct{}{}, struct{}{}, nil
	})
	var bus outbox.LocalBus
	Subscribe(&bus, tst.Engine())
	payload, err := json.Marshal(orders.Event{OrderID: "o1", Email: "a@example.com", State: orders.StateCanceled})
	if err != nil {
		t.Fatal(err)
	}
	msg := outbox.Message{EventID: "e1", Topic: orders.TopicCanceled, Key: "o1", Payload: payload}
	for range 2 {
		if err := bus.Publish(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}
	// A malformed event is dropped, not retried forever.
	if err := bus.Publish(t.Context(), outbox.Message{EventID: "e2", Topic: orders.TopicFailed, Payload: []byte("{")}); err != nil {
		t.Fatal(err)
	}
	tst.Settle()
	want := notifications.Message{EventID: "e1", OrderID: "o1", Email: "a@example.com", Template: "order_canceled"}
	if m := <-got; m != want || len(got) != 0 {
		t.Fatalf("delivered %+v (%d more), want %+v once", m, len(got), want)
	}
}

// An event redelivered after retention purged its workflow is handled
// (nil, so the relay moves on) and not sent again: the engine keeps the
// purged instance id (Options.KeepPurgedIDs).
func TestRedeliveryAfterPurge(t *testing.T) {
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}, KeepPurgedIDs: time.Hour}))
	var sent atomic.Int32
	flowtest.OnActivity(tst, deliver, func(context.Context, flows.Key, notifications.Message) (struct{}, struct{}, error) {
		sent.Add(1)
		return struct{}{}, struct{}{}, nil
	})
	var bus outbox.LocalBus
	Subscribe(&bus, tst.Engine())
	payload, err := json.Marshal(orders.Event{OrderID: "o1", Email: "a@example.com", State: orders.StateCompleted})
	if err != nil {
		t.Fatal(err)
	}
	msg := outbox.Message{EventID: "e1", Topic: orders.TopicCompleted, Key: "o1", Payload: payload}
	if err := bus.Publish(t.Context(), msg); err != nil {
		t.Fatal(err)
	}
	tst.Settle()
	tst.Advance(time.Minute)
	if n, err := tst.Engine().PurgeEnded(t.Context(), 0, 10); err != nil || n != 1 {
		t.Fatalf("purged %d, %v; want 1", n, err)
	}
	if err := bus.Publish(t.Context(), msg); err != nil {
		t.Fatalf("redelivery after the purge: %v", err)
	}
	tst.Settle()
	if sent.Load() != 1 {
		t.Fatalf("sent %d times, want once", sent.Load())
	}
}
