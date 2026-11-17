package adapt

import (
	"errors"
	"fmt"
	"testing"

	"github.com/imdat99/go-flows/flows"
)

var errRejected = errors.New("rejected")

func isRejection(err error) bool { return errors.Is(err, errRejected) }

func TestError(t *testing.T) {
	if Error(nil, isRejection) != nil {
		t.Fatal("nil is nil")
	}
	err := Error(fmt.Errorf("op: %w", errRejected), isRejection)
	if !flows.IsPermanent(err) || !errors.Is(err, errRejected) {
		t.Fatalf("rejection: %v, want permanent and still errors.Is", err)
	}
	if err := Error(errors.New("timeout"), isRejection); flows.IsPermanent(err) {
		t.Fatalf("transient: %v, want retryable", err)
	}
}

func TestRef(t *testing.T) {
	r := NewRef[int]("p")
	if _, err := r.Get(); !errors.Is(err, ErrNotConfigured) || flows.IsPermanent(err) {
		t.Fatalf("unset: %v, want ErrNotConfigured, retryable", err)
	}
	r.Set(7)
	if v, err := r.Get(); err != nil || *v != 7 {
		t.Fatalf("set: %v, %v", v, err)
	}
}
