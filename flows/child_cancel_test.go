package flows

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/testdb"
)

// T12.2: Future.RequestCancel.

type rcIn struct {
	ID         string `json:"id"`
	Kid        string `json:"kid"`         // the child's mode (rcKidWF)
	Early      bool   `json:"early"`       // request in the task that starts the child
	Dup        bool   `json:"dup"`         // request twice
	FailAfter  bool   `json:"fail_after"`  // fail once the child's outcome is in
	ParentStep bool   `json:"parent_step"` // a compensable step before the child
}

var (
	// rcKidWF does step "k", then waits for "go". Canceled, it honours
	// the cancel ("honour"), finishes anyway on a disconnected context
	// ("ignore": step "k2"), or waits for "go" on it ("stall").
	rcKidWF = DefineWorkflow("rc.kid", 1, func(c Ctx, mode string) (string, error) {
		if _, err := compStep.Run(c, "k"); err != nil {
			return "", err
		}
		if _, err := Await[string](c, "go", 0); err == nil || mode == "honour" {
			return "went", err
		}
		dc, cancel := Disconnected(c)
		defer cancel()
		if mode == "stall" {
			if _, err := Await[string](dc, "go", 0); err != nil {
				return "", err
			}
			return "stalled", nil
		}
		if _, err := compStep.Run(dc, "k2"); err != nil {
			return "", err
		}
		return "done anyway", nil
	})
	rcParentWF = DefineWorkflow("rc.parent", 1, func(c Ctx, in rcIn) (string, error) {
		if in.ParentStep {
			if _, err := compStep.Run(c, "p"); err != nil {
				return "", err
			}
		}
		kid := rcKidWF.Child(c, in.ID+"-kid", in.Kid)
		if !in.Early {
			if _, err := Await[string](c, "stop", 0); err != nil {
				return "", err
			}
		}
		kid.RequestCancel(c)
		if in.Dup {
			kid.RequestCancel(c)
		}
		out, err := kid.Get(c)
		if err != nil {
			return "", err
		}
		if in.FailAfter {
			return "", errNoStock
		}
		return out, nil
	})
)

// rcStores are the stores the tests run on: memory, Postgres (fast path)
// and Postgres through pgx (no fast path).
var rcStores = []struct {
	name   string
	engine func(t *testing.T) *Engine
}{
	{"mem", testEngine},
	{"pg-fast", func(t *testing.T) *Engine { t.Helper(); return pgTestEngine(t, true) }},
	{"pg-pgx", func(t *testing.T) *Engine { t.Helper(); return pgTestEngine(t, false) }},
}

func pgTestEngine(t *testing.T, fast bool) *Engine {
	t.Helper()
	pool := testdb.New(t)
	if err := pg.Migrate(bg, pool); err != nil {
		t.Fatal(err)
	}
	if !fast {
		cfg := pool.Config()
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
		p, err := pgxpool.NewWithConfig(bg, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		pool = p
	}
	st := pg.New(pool)
	if st.FastPath() != fast {
		t.Fatalf("fast path %v", st.FastPath())
	}
	e, err := newEngine(st, Options{InstanceID: "test", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e
}

// rcStart starts the parent and, unless early, waits until the child ran
// its step and waits for "go", then signals the parent to request.
func rcStart(t *testing.T, e *Engine, in rcIn) *Handle[string] {
	t.Helper()
	comps.reset(in.ID, in.ID+"-kid")
	h, err := rcParentWF.Start(bg, e, in.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	if !in.Early {
		waitFor(t, func() bool { return historyHas(e, in.ID+"-kid", "ActivityCompleted") })
		if err := e.Signal(bg, in.ID, "stop", ""); err != nil {
			t.Fatal(err)
		}
	}
	return h
}

func countEvents(t *testing.T, e *Engine, id, name string) int {
	t.Helper()
	ex, err := e.st.GetExecution(bg, id)
	if err != nil {
		t.Fatal(err)
	}
	hist, err := e.st.LoadHistory(bg, ex.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range hist {
		if ev.Type.String() == name {
			n++
		}
	}
	return n
}

func kidStatus(t *testing.T, e *Engine, id string) Status {
	t.Helper()
	s, err := rcKidWF.Handle(e, id+"-kid").Status(bg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The child completes after the request (twice, recorded once): the parent
// gets its output, not ErrCanceled.
func TestRequestCancelChildCompletesAnyway(t *testing.T) {
	for _, sc := range rcStores {
		t.Run(sc.name, func(t *testing.T) {
			e := sc.engine(t)
			h := rcStart(t, e, rcIn{ID: "rc-ignore", Kid: "ignore", Dup: true})
			if out, err := h.Wait(waitCtx(t)); err != nil || out != "done anyway" {
				t.Fatalf("parent: %q %v", out, err)
			}
			if s := kidStatus(t, e, "rc-ignore"); s != StatusCompleted {
				t.Fatalf("kid %s", s)
			}
			if n := countEvents(t, e, "rc-ignore", "ChildCancelRequested"); n != 1 {
				t.Fatalf("%d ChildCancelRequested", n)
			}
			wantJournal(t, "rc-ignore-kid")
		})
	}
}

// The child honours the request: the parent gets its canceled error, and
// the child compensated itself.
func TestRequestCancelChildHonoured(t *testing.T) {
	for _, sc := range rcStores {
		t.Run(sc.name, func(t *testing.T) {
			e := sc.engine(t)
			h := rcStart(t, e, rcIn{ID: "rc-honour", Kid: "honour"})
			_, err := h.Wait(waitCtx(t))
			var re *RunError
			if !errors.As(err, &re) || re.Status != StatusFailed || !errors.Is(err, ErrCanceled) {
				t.Fatalf("parent: %v", err)
			}
			if s := kidStatus(t, e, "rc-honour"); s != StatusCanceled {
				t.Fatalf("kid %s", s)
			}
			wantJournal(t, "rc-honour-kid", "undo k")
		})
	}
}

// A request in the task that starts the child: it is never created, and the
// future resolves with ErrCanceled.
func TestRequestCancelChildBeforeStart(t *testing.T) {
	for _, sc := range rcStores {
		t.Run(sc.name, func(t *testing.T) {
			e := sc.engine(t)
			h := rcStart(t, e, rcIn{ID: "rc-early", Kid: "ignore", Early: true})
			if _, err := h.Wait(waitCtx(t)); !errors.Is(err, ErrCanceled) {
				t.Fatalf("parent: %v", err)
			}
			if _, err := e.st.GetExecution(bg, "rc-early-kid"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("kid: %v", err)
			}
			if n := countEvents(t, e, "rc-early", "ChildScheduled"); n != 0 {
				t.Fatalf("%d ChildScheduled", n)
			}
		})
	}
}

// The parent is canceled while it waits for the child it asked to cancel:
// its future resolves with ErrCanceled at once (the workflow ends canceled,
// not with the child's output), and, as for any canceled parent (WithCancel
// or not), its compensation waits for the child still running.
func TestRequestCancelChildParentCanceled(t *testing.T) {
	for _, sc := range rcStores {
		t.Run(sc.name, func(t *testing.T) {
			e := sc.engine(t)
			h := rcStart(t, e, rcIn{ID: "rc-pcancel", Kid: "stall"})
			waitFor(t, func() bool { return historyHas(e, "rc-pcancel", "ChildCancelRequested") })
			if err := e.Cancel(bg, "rc-pcancel"); err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool { return historyHas(e, "rc-pcancel", "ExecutionCanceled") })
			if s := kidStatus(t, e, "rc-pcancel"); s != StatusRunning {
				t.Fatalf("kid %s", s)
			}
			if err := e.Signal(bg, "rc-pcancel-kid", "go", ""); err != nil {
				t.Fatal(err)
			}
			var re *RunError
			if _, err := h.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCanceled {
				t.Fatalf("parent: %v", err)
			}
			// It completed while the parent compensated: compensated with it.
			if s := kidStatus(t, e, "rc-pcancel"); s != StatusCompensated {
				t.Fatalf("kid %s", s)
			}
			wantJournal(t, "rc-pcancel-kid", "undo k")
		})
	}
}

// Saga: a child that completed after the request keeps its compensations;
// the parent failing afterwards compensates it with its own steps.
func TestRequestCancelChildCompletedIsCompensated(t *testing.T) {
	for _, sc := range rcStores {
		t.Run(sc.name, func(t *testing.T) {
			e := sc.engine(t)
			h := rcStart(t, e, rcIn{ID: "rc-saga", Kid: "ignore", FailAfter: true, ParentStep: true})
			var re *RunError
			if _, err := h.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCompensated || !errors.Is(err, errNoStock) {
				t.Fatalf("parent: %v", err)
			}
			wantJournal(t, "rc-saga-kid", "undo k2", "undo k")
			wantJournal(t, "rc-saga", "undo p")
		})
	}
}

// Misuse: RequestCancel on an activity's future fails the workflow.
var rcOnActivityWF = DefineWorkflow("rc.on-activity", 1, func(c Ctx, _ int) (string, error) {
	compStep.Start(c, "x").RequestCancel(c)
	return "", nil
})

func TestRequestCancelOnActivityFails(t *testing.T) {
	e := testEngine(t)
	if _, err := rcOnActivityWF.Run(waitCtx(t), e, "rc-act", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("%v", err)
	}
}
