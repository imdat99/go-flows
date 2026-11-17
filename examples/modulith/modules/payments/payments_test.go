package payments

import (
	"errors"
	"log/slog"
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

func (s *Service) state(t *testing.T, id string) string {
	t.Helper()
	var st string
	if err := s.db.QueryRow(t.Context(), `SELECT state FROM payments.payments WHERE id = $1`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// A capture whose outcome the caller did not know (it timed out, still in
// flight elsewhere) lands after its refund found nothing: Void, the next
// undo, must refund it, and a capture after that must fail.
func TestLateCaptureIsRefundedByVoid(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	const authKey, captureKey = "o1:2:payments.authorize:1", "o1:4:payments.capture:1"

	id, err := s.Authorize(ctx, authKey, "o1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := s.Refund(ctx, captureKey); err != nil || changed { // nothing captured yet
		t.Fatalf("refund: %v, %v", changed, err)
	}
	if err := s.Capture(ctx, captureKey, id); err != nil { // the late attempt lands
		t.Fatal(err)
	}
	if _, err := s.Void(ctx, authKey); err != nil {
		t.Fatal(err)
	}
	if st := s.state(t, id); st != "refunded" {
		t.Fatalf("payment is %s after the undo, want refunded", st)
	}
	// Another late attempt now fails, as a rejection, instead of capturing.
	if err := s.Capture(ctx, captureKey, id); !errors.Is(err, ErrNotCapturable) || !IsRejection(err) {
		t.Fatalf("capture after void: %v, want ErrNotCapturable", err)
	}
	if st := s.state(t, id); st != "refunded" {
		t.Fatalf("payment is %s, want refunded", st)
	}
}

// An authorization voided before it lands: the late attempt is fenced off
// by the tombstone.
func TestLateAuthorizationIsFenced(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	const authKey = "o2:2:payments.authorize:1"
	if _, err := s.Void(ctx, authKey); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authorize(ctx, authKey, "o2", 100); !errors.Is(err, ErrVoided) || !IsRejection(err) {
		t.Fatalf("late authorize: %v, want ErrVoided", err)
	}
}

// Repeats under the same key find their own row.
func TestAuthorizeAndCaptureAreIdempotent(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	const authKey, captureKey = "o3:2:payments.authorize:1", "o3:4:payments.capture:1"
	a1, err := s.Authorize(ctx, authKey, "o3", 100)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := s.Authorize(ctx, authKey, "o3", 100)
	if err != nil || a1 != a2 {
		t.Fatalf("repeat: %q, %v; want %q", a2, err, a1)
	}
	for range 2 {
		if err := s.Capture(ctx, captureKey, a1); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.state(t, a1); st != "captured" {
		t.Fatalf("payment is %s, want captured", st)
	}
	// Another key cannot capture it again.
	if err := s.Capture(ctx, "other", a1); !errors.Is(err, ErrNotCapturable) {
		t.Fatalf("capture under another key: %v, want ErrNotCapturable", err)
	}
}

func TestDeclinedAndInvalidAreRejections(t *testing.T) {
	s := setup(t)
	if _, err := s.Authorize(t.Context(), "k1", "o4", declineAbove+1); !errors.Is(err, ErrDeclined) || !IsRejection(err) {
		t.Fatalf("over the limit: %v, want ErrDeclined", err)
	}
	if _, err := s.Authorize(t.Context(), "k2", "o4", 0); !errors.Is(err, ErrInvalidAmount) || !IsRejection(err) {
		t.Fatalf("zero amount: %v, want ErrInvalidAmount", err)
	}
	if IsRejection(errors.New("connection reset")) {
		t.Fatal("a transient error is not a rejection")
	}
}
