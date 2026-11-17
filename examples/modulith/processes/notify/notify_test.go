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
