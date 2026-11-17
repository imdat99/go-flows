package orders

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/internal/testdb"
)

func setup(t *testing.T) *Service {
	t.Helper()
	pool := testdb.New(t)
	if err := outbox.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func order(id string) Order {
	return Order{ID: id, Email: id + "@example.com", Address: "Hanoi", Amount: 100, Lines: []Line{{SKU: "sku-a", Qty: 1}}}
}

func (s *Service) end(t *testing.T, id, state string) bool {
	t.Helper()
	var ended bool
	err := pgx.BeginFunc(t.Context(), s.db, func(tx pgx.Tx) error {
		var err error
		ended, err = s.End(t.Context(), tx, id, state, "why")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return ended
}

func (s *Service) events(t *testing.T, key string) []string {
	t.Helper()
	rows, err := s.db.Query(t.Context(), `SELECT topic FROM outbox.events WHERE key = $1 ORDER BY id`, key)
	if err != nil {
		t.Fatal(err)
	}
	topics, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return topics
}

func TestRecordAndEnd(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	o := order("o1")
	if placed, err := s.Placed(ctx, o); err != nil || placed {
		t.Fatalf("placed before: %v, %v", placed, err)
	}
	for range 2 { // a repeat finds its row
		if err := s.Record(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	if placed, err := s.Placed(ctx, o); err != nil || !placed {
		t.Fatalf("placed after: %v, %v", placed, err)
	}
	other := o
	other.Amount = 1
	if _, err := s.Placed(ctx, other); !errors.Is(err, ErrOrderConflict) || !IsRejection(err) {
		t.Fatalf("placed with other contents: %v", err)
	}
	if err := s.Record(ctx, other); !errors.Is(err, ErrOrderConflict) {
		t.Fatalf("record with other contents: %v", err)
	}

	if !s.end(t, "o1", StateCompleted) {
		t.Fatal("not ended")
	}
	// Ended once: a second end, or an end of an unknown order, does nothing.
	if s.end(t, "o1", StateFailed) || s.end(t, "o-none", StateFailed) {
		t.Fatal("ended twice")
	}
	if got := s.events(t, "o1"); len(got) != 1 || got[0] != TopicCompleted {
		t.Fatalf("events %v", got)
	}
	if v, ok, err := s.Get(ctx, "o1"); err != nil || !ok || v.State != StateCompleted || v.Reason != "why" {
		t.Fatalf("get: %+v, %v, %v", v, ok, err)
	}
	// The id of an ended order cannot be recorded again, even with the same
	// contents (an execution purged by retention, its id reused).
	if err := s.Record(ctx, o); !errors.Is(err, ErrOrderConflict) {
		t.Fatalf("record after the end: %v", err)
	}
}

func TestValidate(t *testing.T) {
	var s Service
	bad := order("o1")
	bad.Lines = nil
	if err := s.Validate(bad); !errors.Is(err, ErrInvalidOrder) {
		t.Fatalf("no lines: %v", err)
	}
	if err := s.Validate(order("o1")); err != nil {
		t.Fatal(err)
	}
}
