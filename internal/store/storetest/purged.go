package storetest

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Tombstones of purged instance ids (T12.3, store.PurgedKeeper).

// keepPurged turns tombstones on for s, or skips the test.
func keepPurged(t *testing.T, s store.Store, d time.Duration) {
	t.Helper()
	k, ok := s.(store.PurgedKeeper)
	if !ok {
		t.Skip("store does not keep purged ids")
	}
	k.KeepPurged(d)
}

// endAll completes the first task of every claimable execution.
func endAll(t *testing.T, s store.Store, h Harness) {
	t.Helper()
	for _, tk := range claim(t, s, owner, h.TTL) {
		started(t, s, tk, &store.Completion{Status: store.StatusCompleted})
	}
}

func wantPurged(t *testing.T, s store.Store, ne store.NewExecution) {
	t.Helper()
	if _, created, err := s.CreateExecution(ctx, ne); !errors.Is(err, store.ErrInstancePurged) || created {
		t.Fatalf("create %s: created=%v err=%v, want ErrInstancePurged", ne.InstanceID, created, err)
	}
}

// A purged id is refused until forgotten or its tombstone expires;
// tombstones are purged by PurgeEnded, up to its limit.
func testPurgedIDsKept(t *testing.T, h Harness) {
	s := h.New(t)
	keepPurged(t, s, 2*h.TTL)
	for _, id := range []string{"a", "b", "c"} {
		mustCreate(t, s, id)
	}
	endAll(t, s, h)
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, h.TTL/2, 10); err != nil || n != 3 {
		t.Fatalf("purge: %d %v", n, err)
	}
	wantPurged(t, s, newExec("a"))
	other := newExec("a")
	other.Workflow = "other"
	wantPurged(t, s, other) // the id is spent, whatever the workflow
	if _, err := s.GetExecution(ctx, "a"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get purged: %v", err)
	}
	// Forgetting frees the id, once.
	if err := s.ForgetInstance(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "never"} {
		if err := s.ForgetInstance(ctx, id); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("forget %s again: %v", id, err)
		}
	}
	mustCreate(t, s, "a")
	// Before their retention, tombstones stay (whatever olderThan).
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, 100*h.TTL, 10); err != nil || n != 0 {
		t.Fatalf("purge: %d %v", n, err)
	}
	wantPurged(t, s, newExec("b"))
	// After it, each purge drops up to limit of them (not counted).
	h.Advance(t, s, h.TTL+h.TTL/2)
	if n, err := s.PurgeEnded(ctx, 100*h.TTL, 1); err != nil || n != 0 {
		t.Fatalf("purge tombstones: %d %v", n, err)
	}
	freed := 0
	for _, id := range []string{"b", "c"} {
		_, created, err := s.CreateExecution(ctx, newExec(id))
		switch {
		case err == nil && created:
			freed++
		case !errors.Is(err, store.ErrInstancePurged):
			t.Fatalf("create %s: %v %v", id, created, err)
		}
	}
	if freed != 1 {
		t.Fatalf("limit 1 freed %d ids", freed)
	}
	if _, err := s.PurgeEnded(ctx, 100*h.TTL, 10); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"b", "c"} {
		if _, _, err := s.CreateExecution(ctx, newExec(id)); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
}

// Without KeepPurged, no tombstone is written: a purged id starts again
// and ForgetInstance finds nothing.
func testPurgedIDsOffByDefault(t *testing.T, h Harness) {
	s := h.New(t)
	mustCreate(t, s, "x")
	endAll(t, s, h)
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, h.TTL/2, 10); err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if err := s.ForgetInstance(ctx, "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("forget: %v", err)
	}
	mustCreate(t, s, "x")
}

// An instance whose earlier executions (continue-as-new) are purged while
// its latest runs gets no tombstone until the latest is purged too.
func testPurgedIDOfChain(t *testing.T, h Harness) {
	s := h.New(t)
	keepPurged(t, s, 100*h.TTL)
	first := mustCreate(t, s, "chain")
	task := claimOne(t, s, owner, h.TTL)
	started(t, s, task, &store.Completion{
		History: []history.Event{{Type: history.ExecutionContinuedAsNew}},
		Status:  store.StatusContinued, ContinueAsNew: &store.NewExecution{Workflow: "wf", Version: 1, Queue: queue},
	})
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, h.TTL/2, 10); err != nil || n != 1 {
		t.Fatalf("purge first: %d %v", n, err)
	}
	if err := s.ForgetInstance(ctx, "chain"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("tombstone of a live instance: %v", err)
	}
	e, created, err := s.CreateExecution(ctx, newExec("chain"))
	if err != nil || created || e.ID == first.ID || e.Status != store.StatusRunning {
		t.Fatalf("create: %+v %v %v", e, created, err)
	}
	endAll(t, s, h)
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, h.TTL/2, 10); err != nil || n != 1 {
		t.Fatalf("purge latest: %d %v", n, err)
	}
	wantPurged(t, s, newExec("chain"))
}

// A child started with a purged id fails at once, as one in use, with
// KindInstancePurged too.
func testPurgedChildID(t *testing.T, h Harness) {
	s := h.New(t)
	keepPurged(t, s, 100*h.TTL)
	mustCreate(t, s, "kid")
	endAll(t, s, h)
	h.Advance(t, s, h.TTL)
	if n, err := s.PurgeEnded(ctx, h.TTL/2, 10); err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	mustCreate(t, s, "parent")
	task := claimOne(t, s, owner, h.TTL)
	c := &store.Completion{Children: []store.NewExecution{{InstanceID: "kid", Workflow: "cw", Version: 1, Queue: queue, ParentSched: 1}}}
	started(t, s, task, c)
	if !c.Children[0].Taken {
		t.Fatal("purged child id not reported taken")
	}
	tk := claimOne(t, s, owner, h.TTL)
	if tk.Execution.InstanceID != "parent" || len(tk.Pending) != 1 {
		t.Fatalf("claimed %s %+v", tk.Execution.InstanceID, tk.Pending)
	}
	ev := tk.Pending[0].Event
	kinds := strings.Split(ev.ErrKinds, ",")
	if ev.Type != history.ChildFailed || ev.ScheduleID != 1 || ev.Ref != "kid" || !ev.Permanent ||
		len(kinds) != 2 || kinds[0] != store.KindInstanceExists || kinds[1] != store.KindInstancePurged {
		t.Fatalf("parent got %+v", ev)
	}
	if _, err := s.GetExecution(ctx, "kid"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("purged child started: %v", err)
	}
}

// Creates racing purges see each id in use or purged, never free.
func testPurgeRacesCreate(t *testing.T, h Harness) {
	s := h.New(t)
	keepPurged(t, s, 100*h.TTL)
	const n = 24
	for i := range n {
		mustCreate(t, s, "r"+strconv.Itoa(i))
	}
	for { // claims take 10 at most
		tasks := claim(t, s, owner, h.TTL)
		if len(tasks) == 0 {
			break
		}
		for _, tk := range tasks {
			started(t, s, tk, &store.Completion{Status: store.StatusCompleted})
		}
	}
	h.Advance(t, s, h.TTL)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	fail := func(msg string) {
		mu.Lock()
		failures = append(failures, msg)
		mu.Unlock()
	}
	purged := 0
	wg.Go(func() {
		for {
			k, err := s.PurgeEnded(ctx, h.TTL/2, 3)
			if err != nil {
				fail("purge: " + err.Error())
				return
			}
			if k == 0 {
				return
			}
			purged += k
		}
	})
	for w := range 4 {
		wg.Go(func() {
			for round := range 6 {
				for i := range n {
					id := "r" + strconv.Itoa((i+w*5+round)%n)
					_, created, err := s.CreateExecution(ctx, newExec(id))
					if created || (err != nil && !errors.Is(err, store.ErrInstancePurged)) {
						fail(id + ": created=" + strconv.FormatBool(created) + " err=" + errString(err))
					}
				}
			}
		})
	}
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("%d failures, first: %s", len(failures), failures[0])
	}
	if purged != n {
		t.Fatalf("purged %d of %d", purged, n)
	}
	for i := range n {
		wantPurged(t, s, newExec("r"+strconv.Itoa(i)))
	}
}

func errString(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}
