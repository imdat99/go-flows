package payment

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/imdat99/go-flows/examples/modulith/modules/payments"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
	"github.com/imdat99/go-flows/internal/testdb"
)

var errLater = errors.New("a later step failed")

// pay stands for a saga: it authorizes, captures, then fails if asked to,
// which compensates both steps (refund, then void).
var pay = flows.DefineWorkflow("payment.test-pay", 1, func(c flows.Ctx, r AuthorizeRequest) (string, error) {
	auth, err := Authorize.Run(c, r)
	if err != nil {
		return "", err
	}
	if _, err := Capture.Run(c, auth.PaymentID); err != nil {
		return "", err
	}
	if r.OrderID == "fail" {
		return "", errLater
	}
	return auth.PaymentID, nil
}, flows.WithWorkflowQueue(Queue))

func setup(t *testing.T) (*flowtest.Tester, *payments.Service) {
	t.Helper()
	pool := testdb.New(t)
	if err := payments.Migrate(t.Context(), pool); err != nil {
		t.Fatal(err)
	}
	svc, err := payments.NewService(pool, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := Configure(Services{Payments: svc}); err != nil {
		t.Fatal(err)
	}
	return flowtest.New(t, flowtest.WithOptions(flows.Options{Queues: []string{Queue}})), svc
}

// The real adapters on the real module: the activity key is the module's
// idempotency key, so the undo finds the payment by key.
func TestAdaptersCompensateByKey(t *testing.T) {
	tst, svc := setup(t)
	if id, err := flowtest.Run(tst, pay, "pay-ok", AuthorizeRequest{OrderID: "ok", Amount: 100}); err != nil || id == "" {
		t.Fatalf("pay: %q, %v", id, err)
	}
	if p, err := svc.Find(t.Context(), "ok"); err != nil || p.State != "captured" {
		t.Fatalf("payment %+v, %v; want captured", p, err)
	}
	_, err := flowtest.Run(tst, pay, "pay-fail", AuthorizeRequest{OrderID: "fail", Amount: 100})
	if !errors.Is(err, flows.ErrCompensated) { // errLater has no kind: only its text is kept
		t.Fatalf("err = %v, want compensated", err)
	}
	if p, err := svc.Find(t.Context(), "fail"); err != nil || p.State != "refunded" {
		t.Fatalf("payment %+v, %v; want refunded", p, err)
	}
}

// A decline is a rejection of the module: permanent, with its kind.
func TestDeclineIsPermanent(t *testing.T) {
	tst, _ := setup(t)
	_, err := flowtest.Run(tst, pay, "pay-big", AuthorizeRequest{OrderID: "big", Amount: 2_000_000})
	if !errors.Is(err, payments.ErrDeclined) || !flows.IsPermanent(err) {
		t.Fatalf("err = %v, want a permanent decline", err)
	}
}
