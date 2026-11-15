package command

import (
	"errors"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
)

func activity(id int32, name, in string) *Command {
	p := []byte(in)
	return &Command{ID: id, Kind: Activity, Name: name, Payload: p, ArgHash: history.HashArgs(p)}
}

func TestCommitAppendsScheduledEventOnce(t *testing.T) {
	c := activity(1, "charge", `{"amount":10}`)
	evs, err := c.Commit(nil)
	if err != nil || len(evs) != 1 {
		t.Fatalf("commit: %v %v", evs, err)
	}
	ev := evs[0]
	if ev.Type != history.ActivityScheduled || ev.ScheduleID != 1 || ev.Name != "charge" || ev.ArgHash != c.ArgHash {
		t.Fatalf("event %+v", ev)
	}
	if c.State != Committed {
		t.Fatalf("state %v", c.State)
	}
	evs, err = c.Commit(evs)
	if err != nil || len(evs) != 1 {
		t.Fatalf("second commit must append nothing: %v %v", evs, err)
	}
}

func TestCancelBeforeCommitWritesNothing(t *testing.T) {
	c := &Command{ID: 2, Kind: Timer, Name: "t", At: time.Unix(100, 0)}
	if err := c.Cancel(); err != nil {
		t.Fatal(err)
	}
	evs, err := c.Commit(nil)
	if err != nil || len(evs) != 0 || c.State != Dropped {
		t.Fatalf("evs=%v err=%v state=%v", evs, err, c.State)
	}
	if err := c.Finish(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("outcome for a dropped command must be rejected: %v", err)
	}
	ev := history.Event{Type: history.TimerScheduled, ScheduleID: 2}
	if err := c.Replay(&ev); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("recorded event for a dropped command: %v", err)
	}
}

func TestCancelAfterCommitWritesCancelEvent(t *testing.T) {
	for kind, want := range map[Kind]history.Type{
		Activity: history.ActivityCancelRequested,
		Timer:    history.TimerCanceled,
		Child:    history.ChildCancelRequested,
	} {
		c := &Command{ID: 3, Kind: kind, Name: "x"}
		evs, _ := c.Commit(nil)
		if err := c.Cancel(); err != nil {
			t.Fatal(err)
		}
		evs, err := c.Commit(evs)
		if err != nil || len(evs) != 2 || evs[1].Type != want || evs[1].ScheduleID != 3 {
			t.Fatalf("%v: evs=%+v err=%v", kind, evs, err)
		}
		if err := c.Finish(); err != nil || c.State != Done {
			t.Fatalf("%v: outcome after cancel: %v %v", kind, err, c.State)
		}
		if err := c.Cancel(); err != nil {
			t.Fatalf("%v: cancel after done must be a no-op: %v", kind, err)
		}
	}
}

// GW-10: driving a command out of order is an error, never a panic.
func TestGW10_InvalidTransitionsAreErrors(t *testing.T) {
	c := activity(1, "a", "{}")
	if err := c.Finish(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("finish while pending: %v", err)
	}
	se := &Command{ID: 2, Kind: SideEffect}
	if err := se.Cancel(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("cancel side effect: %v", err)
	}
}

// GW-11: replay compares type, name, arguments, timer deadline and child id.
func TestGW11_ReplayDetectsEveryMismatch(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	recorded := func(c *Command) history.Event {
		cp := *c
		evs, _ := cp.Commit(nil)
		return evs[0]
	}
	cases := []struct {
		name     string
		recorded *Command
		issued   *Command
	}{
		{"name", activity(1, "charge", "{}"), activity(1, "refund", "{}")},
		{"arguments", activity(1, "charge", `{"a":1}`), activity(1, "charge", `{"a":2}`)},
		{"type", activity(1, "x", "{}"), &Command{ID: 1, Kind: Timer, Name: "x", At: at}},
		{"timer deadline", &Command{ID: 1, Kind: Timer, At: at}, &Command{ID: 1, Kind: Timer, At: at.Add(time.Second)}},
		{"child id", &Command{ID: 1, Kind: Child, Name: "w", Ref: "a"}, &Command{ID: 1, Kind: Child, Name: "w", Ref: "b"}},
		{"child version", &Command{ID: 1, Kind: Child, Name: "w", Version: 1}, &Command{ID: 1, Kind: Child, Name: "w", Version: 2}},
		{"schedule id", activity(1, "x", "{}"), activity(2, "x", "{}")},
	}
	for _, tc := range cases {
		ev := recorded(tc.recorded)
		if err := tc.issued.Replay(&ev); !errors.Is(err, ErrNonDeterministic) {
			t.Errorf("%s: Replay = %v, want ErrNonDeterministic", tc.name, err)
		}
	}
	same := activity(1, "charge", `{"a":1}`)
	ev := recorded(same)
	issued := activity(1, "charge", `{"a":1}`)
	if err := issued.Replay(&ev); err != nil || issued.State != Committed {
		t.Fatalf("matching replay: %v state %v", err, issued.State)
	}
}

func TestReplayOfCancel(t *testing.T) {
	c := &Command{ID: 4, Kind: Timer, At: time.Unix(5, 0)}
	evs, _ := c.Commit(nil)
	_ = c.Cancel()
	evs, _ = c.Commit(evs)

	r := &Command{ID: 4, Kind: Timer, At: time.Unix(5, 0)}
	if err := r.Replay(&evs[0]); err != nil {
		t.Fatal(err)
	}
	_ = r.Cancel()
	if err := r.Replay(&evs[1]); err != nil || r.State != Canceled {
		t.Fatalf("replay cancel: %v %v", err, r.State)
	}
	// A recorded cancel the replay did not issue is non-determinism.
	r2 := &Command{ID: 4, Kind: Timer, At: time.Unix(5, 0)}
	_ = r2.Replay(&evs[0])
	if err := r2.Replay(&evs[1]); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("unexpected cancel: %v", err)
	}
}

func BenchmarkCommitActivity(b *testing.B) {
	p := []byte(`{"order":"9f1c2a4e","amount":12999}`)
	h := history.HashArgs(p)
	evs := make([]history.Event, 0, 1)
	b.ReportAllocs()
	for range b.N {
		c := Command{ID: 1, Kind: Activity, Name: "charge", Payload: p, ArgHash: h}
		evs, _ = c.Commit(evs[:0])
	}
}

func TestFinishTwiceIsAnError(t *testing.T) {
	c := activity(1, "a", "{}")
	_, _ = c.Commit(nil)
	if err := c.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("second outcome: %v", err)
	}
}

// An outcome arriving in the same task as the cancel: no cancel event is
// written, and replaying that history (scheduled, then outcome) matches.
func TestOutcomeWhileCancelPendingWritesNoCancel(t *testing.T) {
	c := activity(1, "a", "{}")
	evs, _ := c.Commit(nil)
	_ = c.Cancel()
	if err := c.Finish(); err != nil || c.State != Done {
		t.Fatalf("finish while cancel pending: %v %v", err, c.State)
	}
	evs, _ = c.Commit(evs)
	if len(evs) != 1 {
		t.Fatalf("cancel event written after the outcome: %+v", evs)
	}
	r := activity(1, "a", "{}")
	if err := r.Replay(&evs[0]); err != nil {
		t.Fatal(err)
	}
	_ = r.Cancel()
	if err := r.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayOfCancelForActivityAndChild(t *testing.T) {
	for _, mk := range []func() *Command{
		func() *Command { return activity(5, "a", "{}") },
		func() *Command { return &Command{ID: 5, Kind: Child, Name: "w", Ref: "child-1", Version: 2} },
	} {
		c := mk()
		evs, _ := c.Commit(nil)
		_ = c.Cancel()
		evs, _ = c.Commit(evs)
		r := mk()
		if err := r.Replay(&evs[0]); err != nil {
			t.Fatal(err)
		}
		_ = r.Cancel()
		if err := r.Replay(&evs[1]); err != nil || r.State != Canceled {
			t.Fatalf("%v: %v %v", r.Kind, err, r.State)
		}
		if err := r.Replay(&evs[1]); !errors.Is(err, ErrNonDeterministic) {
			t.Fatalf("%v: replaying an event twice: %v", r.Kind, err)
		}
	}
}

func TestSideEffectIsDoneWhenCommittedOrReplayed(t *testing.T) {
	c := &Command{ID: 7, Kind: SideEffect, Payload: []byte("42")}
	evs, err := c.Commit(nil)
	if err != nil || c.State != Done || evs[0].Type != history.SideEffectResult || string(evs[0].Payload) != "42" {
		t.Fatalf("commit: %v %v %+v", err, c.State, evs)
	}
	r := &Command{ID: 7, Kind: SideEffect, Payload: []byte("other")} // value comes from history
	if err := r.Replay(&evs[0]); err != nil || r.State != Done {
		t.Fatalf("replay: %v %v", err, r.State)
	}
}

// A timer deadline with nanoseconds must match its microsecond-precision
// stored form during a cold replay.
func TestTimerDeadlineUsesDatabasePrecision(t *testing.T) {
	at := time.Unix(1_700_000_000, 123_456_789)
	c := &Command{ID: 1, Kind: Timer, At: at}
	evs, _ := c.Commit(nil)
	if !evs[0].At.Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("stored deadline %v", evs[0].At)
	}
	stored := evs[0]
	stored.At = time.UnixMicro(at.UnixMicro()).UTC() // as read back from Postgres
	r := &Command{ID: 1, Kind: Timer, At: at}
	if err := r.Replay(&stored); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownKindIsAnError(t *testing.T) {
	c := &Command{ID: 1}
	if _, err := c.Commit(nil); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("commit: %v", err)
	}
	if err := c.Replay(&history.Event{ScheduleID: 1}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("replay: %v", err)
	}
	if c.Cancelable() {
		t.Fatal("unknown kind must not be cancelable")
	}
}
