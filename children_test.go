package flows

import (
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
