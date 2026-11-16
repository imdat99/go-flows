package flows

import (
	"context"
	"errors"
	"testing"
	"time"
)

var (
	// kidWF does a compensable step, then waits for "go".
	kidWF = DefineWorkflow("tree.kid", 1, func(c Ctx, name string) (string, error) {
		if _, err := compStep.Run(c, name); err != nil {
			return "", err
		}
		v, err := Await[string](c, "go", time.Hour)
		return name + v, err
	})
	// treeWF starts a kid (id-kid) and, if Wait, waits for it; otherwise
	// ends.
	treeWF = DefineWorkflow("tree.parent", 1, func(c Ctx, in treeIn) (string, error) {
		if _, err := compStep.Run(c, "p"); err != nil {
			return "", err
		}
		kid := kidWF.Child(c, in.ID+"-kid", "k")
		if !in.Wait {
			// A child issued in the task the parent ends in is dropped (as an
			// activity would be): let it start first.
			if err := Sleep(c, 20*time.Millisecond); err != nil {
				return "", err
			}
			return "left", nil
		}
		return kid.Get(c)
	})
)

type treeIn struct {
	ID   string `json:"id"`
	Wait bool   `json:"wait"`
}

func startTree(t *testing.T, e *Engine, id string, wait bool) *Handle[string] {
	t.Helper()
	comps.reset(id, id+"-kid")
	h, err := treeWF.Start(bg, e, id, treeIn{ID: id, Wait: wait})
	if err != nil {
		t.Fatal(err)
	}
	if wait {
		waitFor(t, func() bool { return historyHas(e, id+"-kid", "TimerScheduled") })
	}
	return h
}

func kidEnd(t *testing.T, e *Engine, id string) error {
	t.Helper()
	_, err := kidWF.Handle(e, id+"-kid").Wait(waitCtx(t))
	return err
}

// T6.1: canceling a parent cancels its waiting child; both compensate.
func TestCancelByTree(t *testing.T) {
	e := testEngine(t)
	h := startTree(t, e, "tree-cancel", true)
	if err := e.Cancel(bg, "tree-cancel"); err != nil {
		t.Fatal(err)
	}
	var re *RunError
	if _, err := h.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("parent: %v", err)
	}
	if err := kidEnd(t, e, "tree-cancel"); !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("kid: %v", err)
	}
	wantJournal(t, "tree-cancel", "undo p")
	wantJournal(t, "tree-cancel-kid", "undo k")
}

// T6.1: a parent that ends, or is terminated, cancels the children it left
// running.
func TestEndCancelsChildren(t *testing.T) {
	e := testEngine(t)
	h := startTree(t, e, "tree-left", false)
	if out, err := h.Wait(waitCtx(t)); err != nil || out != "left" {
		t.Fatalf("%q %v", out, err)
	}
	var re *RunError
	if err := kidEnd(t, e, "tree-left"); !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("kid of an ended parent: %v", err)
	}
	if j := comps.get("tree-left-kid"); len(j) > 1 || len(j) == 1 && j[0] != "undo k" {
		t.Fatalf("kid compensations %q", j) // "undo k" if its step ran before the cancel
	}

	startTree(t, e, "tree-term", true)
	if err := e.Terminate(bg, "tree-term", "stop"); err != nil {
		t.Fatal(err)
	}
	if err := kidEnd(t, e, "tree-term"); !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("kid of a terminated parent: %v", err)
	}
	wantJournal(t, "tree-term") // terminated: no compensation
	wantJournal(t, "tree-term-kid", "undo k")
}

// A child is addressed by its instance id: signals reach it directly.
func TestSignalChild(t *testing.T) {
	e := testEngine(t)
	h := startTree(t, e, "tree-sig", true)
	if err := e.Signal(bg, "tree-sig-kid", "go", "!"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Wait(waitCtx(t)); err != nil || out != "k!" {
		t.Fatalf("%q %v", out, err)
	}
}

// T6.2: signals racing continue-as-new are all delivered, in order, across
// executions (GW-3).
var tally = DefineWorkflow("tree.tally", 1, func(c Ctx, seen []int) ([]int, error) {
	ch := Signals[int](c, "n")
	for range 3 {
		v, err := ch.Receive(c)
		if err != nil {
			return nil, err
		}
		seen = append(seen, v)
		if len(seen) == 12 {
			return seen, nil
		}
	}
	return nil, ContinueAsNew(c, seen)
})

func TestSignalsAcrossContinueAsNew(t *testing.T) {
	e := testEngine(t)
	h, err := tally.Start(bg, e, "tally", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		if err := e.Signal(bg, "tally", "n", i); err != nil {
			t.Fatal(err)
		}
	}
	seen, err := h.Wait(waitCtx(t))
	if err != nil || len(seen) != 12 {
		t.Fatalf("%v %v", seen, err)
	}
	for i, v := range seen {
		if v != i {
			t.Fatalf("order %v", seen)
		}
	}
}

var (
	nestGrand = DefineWorkflow("nest.grand", 1, func(c Ctx, step string) (string, error) {
		_, err := compStep.Run(c, step)
		return "grand done", err
	})
	// nestKid does step k (and, with Grandchild, starts a grandchild doing
	// step g), then completes.
	nestKid = DefineWorkflow("nest.kid", 1, func(c Ctx, in nestIn) (string, error) {
		if _, err := compStep.Run(c, in.Step); err != nil {
			return "", err
		}
		if in.Grandchild != "" {
			if _, err := nestGrand.Child(c, in.Grandchild, "g").Get(c); err != nil {
				return "", err
			}
		}
		if in.FlakyUndo {
			if _, err := compFlaky.Run(c, "kid-flaky"); err != nil {
				return "", err
			}
		}
		return "kid done", nil
	})
	// nestParent does step a, runs a kid to completion, does step b, fails.
	nestParent = DefineWorkflow("nest.parent", 1, func(c Ctx, in nestIn) (string, error) {
		if _, err := compStep.Run(c, "a"); err != nil {
			return "", err
		}
		if _, err := nestKid.Child(c, in.Kid, nestIn{Step: "k", Grandchild: in.Grandchild, FlakyUndo: in.FlakyUndo}).Get(c); err != nil {
			return "", err
		}
		if _, err := compStep.Run(c, "b"); err != nil {
			return "", err
		}
		return "", errNoStock
	})
)

type nestIn struct {
	Kid        string `json:"kid,omitempty"`
	Grandchild string `json:"grandchild,omitempty"`
	Step       string `json:"step,omitempty"`
	FlakyUndo  bool   `json:"flaky_undo,omitempty"`
}

// T6.3: a failing parent compensates the children that completed (their
// own steps, recursively), in reverse order with its own steps.
func TestNestedCompensation(t *testing.T) {
	e := testEngine(t)
	comps.reset("nest", "nest-kid", "nest-grand")
	_, err := nestParent.Run(waitCtx(t), e, "nest", nestIn{Kid: "nest-kid", Grandchild: "nest-grand"})
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusCompensated || !errors.Is(err, errNoStock) {
		t.Fatalf("parent: %v", err)
	}
	wantJournal(t, "nest", "undo b", "undo a")
	wantJournal(t, "nest-kid", "undo k")
	wantJournal(t, "nest-grand", "undo g")
	// The kid's compensation (with its grandchild's) ran between the
	// parent's b and a: the parent waited for it.
	if _, err := nestKid.Handle(e, "nest-kid").Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCompensated {
		t.Fatalf("kid: %v", err)
	}
	if _, err := nestGrand.Handle(e, "nest-grand").Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCompensated {
		t.Fatalf("grandchild: %v", err)
	}
}

// T6.3: a child whose compensation parks keeps its parent compensating
// until the child is retried.
func TestNestedCompensationWaitsForParkedChild(t *testing.T) {
	flakyFixed.Store(false)
	e := testEngine(t)
	comps.reset("nest-park", "nest-park-kid")
	h, err := nestParent.Start(bg, e, "nest-park", nestIn{Kid: "nest-park-kid", FlakyUndo: true})
	if err != nil {
		t.Fatal(err)
	}
	var re *RunError
	waitFor(t, func() bool {
		st, _ := nestKid.Handle(e, "nest-park-kid").Status(bg)
		return st == StatusNeedsAttention
	})
	if _, err := nestKid.Handle(e, "nest-park-kid").Wait(waitCtx(t)); !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("kid: %v", err)
	}
	if st, _ := h.Status(bg); st != StatusCompensating {
		t.Fatalf("parent %s", st)
	}
	wantJournal(t, "nest-park", "undo b")
	flakyFixed.Store(true)
	if err := e.RetryCompensation(bg, "nest-park-kid"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCompensated {
		t.Fatalf("parent: %v", err)
	}
	wantJournal(t, "nest-park", "undo b", "undo a")
	wantJournal(t, "nest-park-kid", "undo flaky kid-flaky", "undo k")
}

var (
	abandonSlow = DefineActivity("abandon.slow", func(_ context.Context, _ Key, s string) (string, string, error) {
		time.Sleep(200 * time.Millisecond)
		return s, s, nil
	}, func(_ context.Context, k Key, s string) error {
		comps.add(k, "undo slow "+s)
		return nil
	})
	abandonGrand = DefineWorkflow("abandon.grand", 1, func(c Ctx, step string) (string, error) {
		_, err := abandonSlow.Run(c, step)
		return "grand done", err
	})
	// abandonKid starts something it does not await (an activity, or a
	// grandchild), then completes.
	abandonKid = DefineWorkflow("abandon.kid", 1, func(c Ctx, mode string) (string, error) {
		if mode == "act" {
			abandonSlow.Start(c, "s")
		} else {
			abandonGrand.Child(c, "abandon-grand-"+mode, "g")
		}
		if _, err := compStep.Run(c, "k"); err != nil {
			return "", err
		}
		return "kid done", nil
	})
	abandonParent = DefineWorkflow("abandon.parent", 1, func(c Ctx, mode string) (string, error) {
		if _, err := abandonKid.Child(c, "abandon-kid-"+mode, mode).Get(c); err != nil {
			return "", err
		}
		if err := Sleep(c, 400*time.Millisecond); err != nil { // what the kid left ends meanwhile
			return "", err
		}
		return "", errNoStock
	})
)

// Review of T6.3 (H1): a reopened child does not wait for what it left
// running when it completed (an activity, a grandchild): no outcome can
// reach it any more.
func TestReopenedChildWaitsForNothingAbandoned(t *testing.T) {
	for _, mode := range []string{"act", "child"} {
		e := testEngine(t)
		comps.reset("abandon-p-"+mode, "abandon-kid-"+mode)
		_, err := abandonParent.Run(waitCtx(t), e, "abandon-p-"+mode, mode)
		var re *RunError
		if !errors.As(err, &re) || re.Status != StatusCompensated {
			st, _ := abandonKid.Handle(e, "abandon-kid-"+mode).Status(bg)
			t.Fatalf("%s: parent %v; kid %s", mode, err, st)
		}
		wantJournal(t, "abandon-kid-"+mode, "undo k")
	}
}
