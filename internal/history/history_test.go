package history

import (
	"hash/fnv"
	"testing"
)

func TestHashArgsMatchesFNV1a(t *testing.T) {
	for _, in := range [][]byte{nil, []byte(""), []byte(`{"id":42}`), make([]byte, 4096)} {
		h := fnv.New64a()
		_, _ = h.Write(in)
		if got, want := HashArgs(in), h.Sum64(); got != want {
			t.Fatalf("HashArgs(%q) = %x, want %x", in, got, want)
		}
	}
	if HashArgs([]byte(`{"id":1}`)) == HashArgs([]byte(`{"id":2}`)) {
		t.Fatal("different inputs hash the same")
	}
}

func TestTypeNamesAndClasses(t *testing.T) {
	for ty := Type(1); ty < typeCount; ty++ {
		if typeNames[ty] == "" {
			t.Errorf("type %d has no name", ty)
		}
		if !ty.Valid() {
			t.Errorf("%v not valid", ty)
		}
	}
	if Type(0).Valid() || typeCount.Valid() {
		t.Error("out-of-range types must be invalid")
	}
	if got := Type(200).String(); got != "Type(200)" {
		t.Errorf("unknown type string %q", got)
	}
	if !ActivityScheduled.IsCommand() || ActivityCompleted.IsCommand() {
		t.Error("IsCommand classification")
	}
	if !ExecutionContinuedAsNew.IsTerminal() || TaskStarted.IsTerminal() {
		t.Error("IsTerminal classification")
	}
}

// Stored values must never change: they are persisted in the database.
func TestTypeValuesAreStable(t *testing.T) {
	stable := map[Type]uint8{
		ExecutionStarted: 1, ExecutionCompleted: 2, ExecutionFailed: 3, ExecutionContinuedAsNew: 4,
		ExecutionCanceled: 5, ExecutionTerminated: 6, ExecutionCancelRequested: 7, TaskStarted: 8,
		ActivityScheduled: 9, ActivityCompleted: 10, ActivityFailed: 11, ActivityCancelRequested: 12,
		TimerScheduled: 13, TimerFired: 14, TimerCanceled: 15, SignalReceived: 16, SideEffectResult: 17,
		ChildScheduled: 18, ChildCompleted: 19, ChildFailed: 20, ChildCancelRequested: 21,
		CompensationScheduled: 22, CompensationCompleted: 23, CompensationFailed: 24, CompensationRetryRequested: 25,
	}
	for ty, v := range stable {
		if uint8(ty) != v {
			t.Errorf("%v = %d, persisted value is %d", ty, ty, v)
		}
	}
}

func BenchmarkHashArgs(b *testing.B) {
	p := []byte(`{"order_id":"9f1c2a4e-5b6d-4c3e-8f7a-1b2c3d4e5f60","amount":12999,"currency":"VND"}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	for range b.N {
		_ = HashArgs(p)
	}
}
