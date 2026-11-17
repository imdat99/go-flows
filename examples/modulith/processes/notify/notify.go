// Package notify is the process that tells customers how their orders
// ended: it consumes the orders module's events and has the notifications
// module send a message for each.
//
// Each event starts a small workflow whose instance id is the event id, so
// an event delivered twice (the relay delivers at least once) sends one
// message, and a failing mail server is retried by the engine. The two
// modules know nothing of each other: this package translates an
// orders.Event into a notifications.Message.
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/examples/modulith/modules/notifications"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/processes/internal/adapt"
	"github.com/imdat99/go-flows/flows"
)

// Queue is the queue of the workflow and its activity: the processes that
// run the notifications module serve it.
const Queue = "notifications"

// Services are the module services the activity calls.
type Services struct {
	Notifications *notifications.Service
	Logger        *slog.Logger
}

var deps = adapt.NewRef[Services]("notify")

// Configure sets the services. Call it before the engine starts, in every
// process.
func Configure(s Services) error {
	if s.Notifications == nil {
		return errors.New("notify: Configure: Notifications is required")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	deps.Set(s)
	return nil
}

var (
	deliver = flows.DefineActivity("notifications.deliver", deliverFn, nil,
		flows.WithQueue(Queue),
		// The mail server may be down for a while: retry for about an hour.
		flows.WithRetry(flows.Exponential(15, time.Second, 10*time.Minute)),
		flows.WithTimeout(30*time.Second))

	send = flows.DefineWorkflow("notifications.send", 1, func(c flows.Ctx, m notifications.Message) (struct{}, error) {
		return deliver.Run(c, m)
	}, flows.WithWorkflowQueue(Queue))
)

// deliverFn sends once per event id (the module's idempotency key).
func deliverFn(ctx context.Context, _ flows.Key, m notifications.Message) (struct{}, struct{}, error) {
	s, err := deps.Get()
	if err != nil {
		return struct{}{}, struct{}{}, err
	}
	return struct{}{}, struct{}{}, s.Notifications.Deliver(ctx, m)
}

// Subscriber is the part of a bus consumers register on.
type Subscriber interface {
	Subscribe(topic string, h outbox.Handler)
}

// Subscribe registers the consumers of the orders events. They only start
// workflows: e can be any engine on the database.
func Subscribe(bus Subscriber, e *flows.Engine) {
	h := func(ctx context.Context, m outbox.Message) error {
		var ev orders.Event
		if err := json.Unmarshal(m.Payload, &ev); err != nil {
			// A malformed event will never decode: log it and move on
			// rather than block the key forever.
			if s, serr := deps.Get(); serr == nil {
				s.Logger.LogAttrs(ctx, slog.LevelError, "notify: bad event",
					slog.String("event_id", m.EventID), slog.Any("error", err))
			}
			return nil
		}
		_, err := send.Start(ctx, e, "notifications-"+m.EventID, notifications.Message{
			EventID: m.EventID, OrderID: ev.OrderID, Email: ev.Email, Template: "order_" + ev.State,
		})
		return err
	}
	for _, topic := range []string{orders.TopicCompleted, orders.TopicCanceled, orders.TopicFailed} {
		bus.Subscribe(topic, h)
	}
}
