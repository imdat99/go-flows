package compensation

import (
	"testing"

	"github.com/imdat99/go-flows/internal/history"
)

func sched(id int32, name string) history.Event {
	return history.Event{Type: history.ActivityScheduled, ScheduleID: id, Name: name, Queue: "q"}
}

func done(id int32, data string) history.Event {
	ev := history.Event{Type: history.ActivityCompleted, ScheduleID: id}
	if data != "" {
		ev.CompData = []byte(data)
	}
	return ev
}

func failed(id int32, uncertain bool) history.Event {
	return history.Event{Type: history.ActivityFailed, ScheduleID: id, Uncertain: uncertain, Err: "x"}
}

var failEnd = history.Event{Type: history.ExecutionFailed, Err: "boom", ErrKinds: "k"}

func build(evs ...history.Event) *Ledger {
	l := New()
	for i := range evs {
		l.Add(&evs[i])
	}
	return l
}

func TestReverseOutcomeOrder(t *testing.T) {
	// 1 and 2 run concurrently, 2 finishes first; 3 failed for certain.
	l := build(sched(1, "a"), sched(2, "b"), done(2, "B"), done(1, "A"), sched(3, "c"), failed(3, false), failEnd)
	var got []int32
	for {
		a, s := l.Next()
		if a == Finish {
			break
		}
		if a != Schedule {
			t.Fatalf("action %v", a)
		}
		got = append(got, s.ScheduleID)
		ev := history.Event{Type: history.CompensationScheduled, ScheduleID: s.ScheduleID, Name: s.Name}
		l.Add(&ev)
		if a, _ := l.Next(); a != Wait {
			t.Fatalf("while running: %v", a)
		}
		ev = history.Event{Type: history.CompensationCompleted, ScheduleID: s.ScheduleID}
		l.Add(&ev)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 || !l.Ran() {
		t.Fatalf("order %v, want [1 2]", got)
	}
	end := l.End()
	if end == nil || end.Err != "boom" || end.ErrKinds != "k" {
		t.Fatalf("end %+v", end)
	}
}

func TestStepCarriesDataAndUncertainty(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), sched(2, "b"),
		history.Event{Type: history.ActivityFailed, ScheduleID: 2, Uncertain: true, CompData: []byte("null")}, failEnd)
	a, s := l.Next()
	if a != Schedule || s.ScheduleID != 2 || !s.Uncertain || string(s.Data) != "null" || s.Name != "b" || s.Queue != "q" {
		t.Fatalf("%v %+v", a, s)
	}
}

// Only outcomes recorded with compensation data (by a binary that had a
// compensator) are compensated; the planner's registry plays no part.
func TestOnlyOutcomesWithDataAreCompensated(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), sched(2, "b"), done(2, ""), failEnd)
	if a, s := l.Next(); a != Schedule || s.ScheduleID != 1 {
		t.Fatalf("%v %+v", a, s)
	}
	l = build(sched(1, "a"), failed(1, true), failEnd) // uncertain, no compensator
	if !l.Trivial() {
		t.Fatal("not trivial")
	}
	if a, _ := l.Next(); a != Finish || l.Ran() {
		t.Fatalf("%v ran=%v", a, l.Ran())
	}
}

func TestNothingToCompensate(t *testing.T) {
	if !build(sched(1, "a"), done(1, ""), failEnd).Trivial() {
		t.Fatal("not trivial")
	}
	if build(sched(1, "a"), done(1, "null"), failEnd).Trivial() {
		t.Fatal("trivial with compensation data")
	}
}

// Every running activity is awaited (its outcome tells whether it has a
// compensator); late outcomes are compensated first.
func TestWaitsForInFlightActivities(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), sched(2, "a"), sched(3, "nocomp"), failEnd)
	if got := l.Running(); len(got) != 2 || l.Trivial() {
		t.Fatalf("running %v", got)
	}
	if a, _ := l.Next(); a != Wait {
		t.Fatalf("with 2 in flight: %v", a)
	}
	ev := done(2, "A2")
	l.Add(&ev)
	if a, _ := l.Next(); a != Wait {
		t.Fatalf("with 1 in flight: %v", a)
	}
	ev = done(3, "")
	l.Add(&ev)
	if a, s := l.Next(); a != Schedule || s.ScheduleID != 2 {
		t.Fatalf("%v %+v", a, s)
	}
	// A running activity that ends without data leaves nothing to do.
	l = build(sched(1, "a"), failEnd)
	ev = failed(1, false)
	l.Add(&ev)
	if a, _ := l.Next(); a != Finish || l.Ran() {
		t.Fatalf("%v ran=%v", a, l.Ran())
	}
}

func TestParkAndRetry(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), failEnd,
		history.Event{Type: history.CompensationScheduled, ScheduleID: 1},
		history.Event{Type: history.CompensationFailed, ScheduleID: 1, Err: "nope"})
	a, s := l.Next()
	if a != Park || s.ScheduleID != 1 || s.Name != "a" || s.Err != "nope" {
		t.Fatalf("%v %+v", a, s)
	}
	ev := history.Event{Type: history.CompensationRetryRequested}
	l.Add(&ev)
	if a, s = l.Next(); a != Schedule || s.ScheduleID != 1 || string(s.Data) != "A" {
		t.Fatalf("after retry %v %+v", a, s)
	}
}

func TestOpenWork(t *testing.T) {
	l := build(sched(1, "a"),
		history.Event{Type: history.TimerScheduled, ScheduleID: 2},
		history.Event{Type: history.TimerScheduled, ScheduleID: 3},
		history.Event{Type: history.TimerFired, ScheduleID: 3},
		history.Event{Type: history.ChildScheduled, ScheduleID: 4, Ref: "kid"},
		history.Event{Type: history.ChildScheduled, ScheduleID: 5, Ref: "done-kid"},
		history.Event{Type: history.ChildCompleted, ScheduleID: 5},
		failEnd)
	if r := l.Running(); len(r) != 1 || r[0] != 1 {
		t.Fatalf("running %v", r)
	}
	if tm := l.Timers(); len(tm) != 1 || tm[0] != 2 {
		t.Fatalf("timers %v", tm)
	}
	if c := l.Children(); len(c) != 1 || c[0] != "kid" {
		t.Fatalf("children %v", c)
	}
}

func TestCanceledEnd(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), history.Event{Type: history.ExecutionCanceled, Err: "canceled"})
	if e := l.End(); e == nil || e.Type != history.ExecutionCanceled {
		t.Fatalf("end %+v", e)
	}
	if New().End() != nil {
		t.Fatal("end without one")
	}
}

func child(id int32, ref string) history.Event {
	return history.Event{Type: history.ChildScheduled, ScheduleID: id, Name: "cw", Ref: ref}
}

// T6.3: completed children are compensation steps, in outcome order; a
// failed child compensated itself; a running child is awaited.
func TestChildrenAreCompensated(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), child(2, "kid"), history.Event{Type: history.ChildCompleted, ScheduleID: 2},
		child(3, "bad"), history.Event{Type: history.ChildFailed, ScheduleID: 3}, child(4, "slow"), failEnd)
	if l.Trivial() {
		t.Fatal("trivial with a completed child")
	}
	if r := l.Running(); len(r) != 0 {
		t.Fatalf("running children are not activities to cancel: %v", r)
	}
	if a, _ := l.Next(); a != Wait {
		t.Fatalf("with a running child: %v", a)
	}
	ev := history.Event{Type: history.ChildFailed, ScheduleID: 4}
	l.Add(&ev)
	a, s := l.Next()
	if a != Schedule || !s.Child || s.ScheduleID != 2 || s.Ref != "kid" || s.Name != "cw" {
		t.Fatalf("%v %+v", a, s)
	}
	ev = history.Event{Type: history.CompensationScheduled, ScheduleID: 2}
	l.Add(&ev)
	ev = history.Event{Type: history.CompensationCompleted, ScheduleID: 2}
	l.Add(&ev)
	if a, s = l.Next(); a != Schedule || s.Child || s.ScheduleID != 1 {
		t.Fatalf("%v %+v", a, s)
	}
	// Only failed children: nothing to do.
	if !build(child(1, "x"), history.Event{Type: history.ChildFailed, ScheduleID: 1}, failEnd).Trivial() {
		t.Fatal("not trivial with only a failed child")
	}
}

// Review of T6.3 (H1): a completed execution reopened for compensation
// waits for nothing it left running: its open children become child steps
// (the store answers for those that did not complete), its open
// activities are settled.
func TestCompletedExecutionLeavesNothingRunning(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), sched(2, "slow"), child(3, "kid"),
		history.Event{Type: history.ExecutionCompleted}, history.Event{Type: history.ExecutionCompensationRequested})
	a, s := l.Next()
	if a != Schedule || !s.Child || s.ScheduleID != 3 {
		t.Fatalf("%v %+v", a, s)
	}
	ev := history.Event{Type: history.CompensationScheduled, ScheduleID: 3}
	l.Add(&ev)
	ev = history.Event{Type: history.CompensationCompleted, ScheduleID: 3}
	l.Add(&ev)
	if a, s = l.Next(); a != Schedule || s.ScheduleID != 1 {
		t.Fatalf("%v %+v", a, s)
	}
}

// A completed execution asked to compensate plans like a failed one.
func TestCompensationRequested(t *testing.T) {
	l := build(sched(1, "a"), done(1, "A"), history.Event{Type: history.ExecutionCompleted},
		history.Event{Type: history.ExecutionCompensationRequested})
	if e := l.End(); e == nil || e.Type != history.ExecutionCompensationRequested {
		t.Fatalf("end %+v", e)
	}
	if a, s := l.Next(); a != Schedule || s.ScheduleID != 1 {
		t.Fatalf("%v %+v", a, s)
	}
}
