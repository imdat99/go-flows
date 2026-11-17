package notifications

import (
	"log/slog"
	"slices"
	"testing"

	"github.com/imdat99/go-flows/internal/testdb"
)

func TestDeliverOncePerEvent(t *testing.T) {
	pool := testdb.New(t)
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	m := Message{EventID: "e1", OrderID: "o1", Email: "a@example.com", Template: "order_completed"}
	for range 2 {
		if err := s.Deliver(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Sent(t.Context(), "o1")
	if err != nil || !slices.Equal(got, []string{"order_completed"}) {
		t.Fatalf("sent %v, %v; want one message", got, err)
	}
}
