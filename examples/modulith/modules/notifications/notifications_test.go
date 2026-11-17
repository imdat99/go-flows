package notifications

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

func TestSendRetriesTheMailServer(t *testing.T) {
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}}))
	var attempts atomic.Int32
	flowtest.OnActivity(tst, deliver, func(context.Context, flows.Key, Message) (struct{}, struct{}, error) {
		if attempts.Add(1) < 3 {
			return struct{}{}, struct{}{}, errors.New("smtp: 421 try again later")
		}
		return struct{}{}, struct{}{}, nil
	})
	start := tst.Now()
	if _, err := flowtest.Run(tst, send, "notifications-e1", Message{EventID: "e1", Template: "order_completed"}); err != nil {
		t.Fatal(err)
	}
	// Two failed attempts, retried after 1s and 2s of virtual time.
	if attempts.Load() != 3 || tst.Now().Sub(start) < 3*time.Second {
		t.Fatalf("%d attempts in %v", attempts.Load(), tst.Now().Sub(start))
	}
}
