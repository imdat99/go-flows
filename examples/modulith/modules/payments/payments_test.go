package payments

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

func setup(t *testing.T) {
	t.Helper()
	pool := testdb.New(t)
	if err := Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	if err := Configure(Config{DB: pool, Logger: slog.New(slog.DiscardHandler)}); err != nil {
		t.Fatal(err)
	}
}

func state(t *testing.T, id string) string {
	t.Helper()
	c, _ := cfg.Get()
	var s string
	if err := c.DB.QueryRow(t.Context(), `SELECT state FROM payments.payments WHERE id = $1`, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// An uncertain capture (its attempt timed out, still running elsewhere)
// lands after its refund found nothing: void, the next compensator, must
// refund it, and a capture after that must fail.
func TestLateCaptureIsRefundedByVoid(t *testing.T) {
	setup(t)
	ctx := t.Context()
	const authKey, captureKey = flows.Key("o1:2:payments.authorize:1"), flows.Key("o1:4:payments.capture:1")

	auth, _, err := authorize(ctx, authKey, AuthorizeRequest{OrderID: "o1", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := refund(ctx, captureKey, ""); err != nil { // nothing captured yet
		t.Fatal(err)
	}
	if _, _, err := capture(ctx, captureKey, auth.PaymentID); err != nil { // the late attempt lands
		t.Fatal(err)
	}
	if err := void(ctx, authKey, auth.PaymentID); err != nil {
		t.Fatal(err)
	}
	if s := state(t, auth.PaymentID); s != "refunded" {
		t.Fatalf("payment is %s after compensation, want refunded", s)
	}
	// Another late attempt now fails instead of capturing again.
	if _, _, err := capture(ctx, captureKey, auth.PaymentID); err == nil || !flows.IsPermanent(err) {
		t.Fatalf("capture after void: %v, want a permanent error", err)
	}
	if s := state(t, auth.PaymentID); s != "refunded" {
		t.Fatalf("payment is %s, want refunded", s)
	}
}

// An uncertain authorization voided before it lands: the late attempt is
// fenced off by the tombstone.
func TestLateAuthorizationIsFenced(t *testing.T) {
	setup(t)
	ctx := t.Context()
	const authKey = flows.Key("o2:2:payments.authorize:1")
	if err := void(ctx, authKey, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := authorize(ctx, authKey, AuthorizeRequest{OrderID: "o2", Amount: 100}); !errors.Is(err, errFenced) {
		t.Fatalf("late authorize: %v, want errFenced", err)
	}
}

// Retries of the same attempt find their own row.
func TestAuthorizeAndCaptureAreIdempotent(t *testing.T) {
	setup(t)
	ctx := t.Context()
	const authKey, captureKey = flows.Key("o3:2:payments.authorize:1"), flows.Key("o3:4:payments.capture:1")
	a1, _, err := authorize(ctx, authKey, AuthorizeRequest{OrderID: "o3", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	a2, _, err := authorize(ctx, authKey, AuthorizeRequest{OrderID: "o3", Amount: 100})
	if err != nil || a1 != a2 {
		t.Fatalf("retry: %+v, %v; want %+v", a2, err, a1)
	}
	for range 2 {
		if _, _, err := capture(ctx, captureKey, a1.PaymentID); err != nil {
			t.Fatal(err)
		}
	}
	if s := state(t, a1.PaymentID); s != "captured" {
		t.Fatalf("payment is %s, want captured", s)
	}
}
