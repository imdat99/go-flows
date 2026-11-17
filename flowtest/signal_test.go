package flowtest_test

import (
	"context"
	"errors"
	"testing"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

// approver approves the approval workflow it is given, from workflow code.
var approver = flows.DefineWorkflow("ft.approver", 1, func(c flows.Ctx, target string) (bool, error) {
	if _, err := flows.SignalWorkflow(c, target, "approved", true).Get(c); err != nil {
		return false, err
	}
	_, err := flows.SignalWorkflow(c, "ft-nobody", "approved", true).Get(c)
	return errors.Is(err, flows.ErrNotFound), nil
})

// T12.1: SignalWorkflow under the tester (memory store, virtual time).
func TestSignalWorkflow(t *testing.T) {
	tst := flowtest.New(t)
	h, err := approval.Start(context.Background(), tst.Engine(), "ft-approval-sig", 0)
	if err != nil {
		t.Fatal(err)
	}
	tst.Settle()
	if ok, err := flowtest.Run(tst, approver, "ft-approver", "ft-approval-sig"); err != nil || !ok {
		t.Fatalf("approver: %v %v", ok, err)
	}
	tst.Settle()
	if ok, err := h.Wait(context.Background()); err != nil || !ok {
		t.Fatalf("approval: %v %v", ok, err)
	}
}
