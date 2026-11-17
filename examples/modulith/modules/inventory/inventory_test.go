package inventory

import (
	"errors"
	"log/slog"
	"slices"
	"testing"

	"github.com/imdat99/go-flows/internal/testdb"
)

func setup(t *testing.T) *Service {
	t.Helper()
	pool := testdb.New(t)
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	s, err := NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *Service) level(t *testing.T, sku string) int {
	t.Helper()
	n, err := s.Level(t.Context(), sku)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The demo migration stocks sku-apple with 50 and sku-plum with 3.

func TestReserveOncePerKey(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	id1, err := s.Reserve(ctx, "k1", "o1", "sku-apple", 2)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.Reserve(ctx, "k1", "o1", "sku-apple", 2)
	if err != nil || id2 != id1 {
		t.Fatalf("repeat: %d, %v; want %d", id2, err, id1)
	}
	if n := s.level(t, "sku-apple"); n != 48 {
		t.Fatalf("level %d, want 48 (reserved once)", n)
	}
	for range 2 {
		if err := s.Release(ctx, id1); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.level(t, "sku-apple"); n != 50 {
		t.Fatalf("level %d, want 50 (released once)", n)
	}
}

func TestReserveRejections(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	if _, err := s.Reserve(ctx, "k1", "o1", "sku-plum", 4); !errors.Is(err, ErrOutOfStock) || !IsRejection(err) {
		t.Fatalf("over the level: %v, want ErrOutOfStock", err)
	}
	if _, err := s.Reserve(ctx, "k2", "o1", "sku-none", 1); !errors.Is(err, ErrUnknownSKU) || !IsRejection(err) {
		t.Fatalf("unknown sku: %v, want ErrUnknownSKU", err)
	}
	// A failed reservation leaves its key free: the same key can succeed.
	if _, err := s.Reserve(ctx, "k1", "o1", "sku-plum", 3); err != nil {
		t.Fatalf("retry after a rejection: %v", err)
	}
}

// A reservation whose outcome the caller does not know: settled before it
// lands, the late attempt is fenced off; settled after, it is released.
func TestSettleReservation(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	if err := s.SettleReservation(ctx, "late"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reserve(ctx, "late", "o1", "sku-apple", 1); !errors.Is(err, ErrAborted) || !IsRejection(err) {
		t.Fatalf("late reservation: %v, want ErrAborted", err)
	}
	if _, err := s.Reserve(ctx, "landed", "o2", "sku-apple", 5); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.SettleReservation(ctx, "landed"); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.level(t, "sku-apple"); n != 50 {
		t.Fatalf("level %d, want 50", n)
	}
}

func TestAdjustmentsByRequestID(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	a := Adjustment{RequestID: "r1", SKU: "sku-new", Delta: 7, Reason: "count"}
	if applied, err := s.Applied(ctx, a); err != nil || applied {
		t.Fatalf("applied before: %v, %v", applied, err)
	}
	if n, err := s.ApplyAdjustment(ctx, "key-1", a); err != nil || n != 7 {
		t.Fatalf("adjust: %d, %v", n, err)
	}
	// The same key again: the stored result, nothing applied twice.
	if n, err := s.ApplyAdjustment(ctx, "key-1", a); err != nil || n != 7 {
		t.Fatalf("repeat: %d, %v", n, err)
	}
	if applied, err := s.Applied(ctx, a); err != nil || !applied {
		t.Fatalf("applied after: %v, %v", applied, err)
	}
	other := a
	other.Delta = 8
	if _, err := s.Applied(ctx, other); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("other contents: %v, want ErrRequestConflict", err)
	}
	// Another key with a used request id: refused by the adjustments table.
	if _, err := s.ApplyAdjustment(ctx, "key-2", other); !errors.Is(err, ErrRequestConflict) || !IsRejection(err) {
		t.Fatalf("reused request id: %v, want ErrRequestConflict", err)
	}
	if _, err := s.ApplyAdjustment(ctx, "key-3", Adjustment{RequestID: "r2", SKU: "sku-new", Delta: -8}); !errors.Is(err, ErrOutOfStock) {
		t.Fatalf("below zero: %v, want ErrOutOfStock", err)
	}
	if n := s.level(t, "sku-new"); n != 7 {
		t.Fatalf("level %d, want 7", n)
	}
}

func TestDiscontinue(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	if err := s.Discontinue(ctx, "sku-pear"); err != nil {
		t.Fatal(err)
	}
	if err := s.Discontinue(ctx, "sku-none"); !errors.Is(err, ErrUnknownSKU) {
		t.Fatalf("unknown: %v", err)
	}
	skus, err := s.Replenishable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(skus, []string{"sku-apple", "sku-plum"}) {
		t.Fatalf("replenishable %v", skus)
	}
	if st, err := s.Stock(ctx, "sku-pear"); err != nil || !st.Discontinued {
		t.Fatalf("stock %+v, %v", st, err)
	}
}
