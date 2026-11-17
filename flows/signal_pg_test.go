package flows_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/testdb"
)

// pgPinger sends its peer n signals, each before waiting for the
// peer's: two peers complete tasks that signal each other at the same
// time.
var pgPinger = flows.DefineWorkflow("pgtest.pinger", 1, func(c flows.Ctx, in pingIn) (int, error) {
	pings := flows.Signals[int](c, "ping")
	if _, err := flows.Await[struct{}](c, "go", 0); err != nil { // both started
		return 0, err
	}
	for i := range in.N {
		if _, err := flows.SignalWorkflow(c, in.Peer, "ping", i).Get(c); err != nil {
			return 0, err
		}
		if _, err := pings.Receive(c); err != nil {
			return 0, err
		}
	}
	_, err := flows.SignalWorkflow(c, "pg-nobody", "ping", 0).Get(c)
	if !errors.Is(err, flows.ErrNotFound) {
		return 0, fmt.Errorf("to an unknown instance: %w", err)
	}
	return in.N, nil
})

type pingIn struct {
	Peer string `json:"peer"`
	N    int    `json:"n"`
}

// T12.1 on Postgres: pairs of workflows signalling each other back and
// forth, on two engines (fast path, cache on and off): every signal is
// delivered once, none deadlocks.
func TestPostgresSignalWorkflow(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var engines []*flows.Engine
	for i, cache := range []int{0, -1} {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-sig-" + strconv.Itoa(i), PollMax: 50 * time.Millisecond, CacheSize: cache})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Stop(ctx) })
		engines = append(engines, e)
	}
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	const pairs, n = 4, 8
	var wg sync.WaitGroup
	errs := make(chan error, 2*pairs)
	var ids []string
	for p := range pairs {
		a, b := "pg-ping-a"+strconv.Itoa(p), "pg-ping-b"+strconv.Itoa(p)
		for i, id := range []string{a, b} {
			peer := []string{b, a}[i]
			h, err := pgPinger.Start(wctx, engines[i], id, pingIn{Peer: peer, N: n})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
			wg.Go(func() {
				if _, err := h.Wait(wctx); err != nil {
					errs <- fmt.Errorf("%s: %w", id, err)
				}
			})
		}
	}
	for _, id := range ids {
		if err := engines[0].Signal(wctx, id, "go", struct{}{}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var sent, received int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE type = $1), count(*) FILTER (WHERE type = $2)
		FROM flows.history`, int16(history.SignalSent), int16(history.SignalReceived)).Scan(&sent, &received); err != nil {
		t.Fatal(err)
	}
	// Each instance sends n signals to its peer and one to nobody, and
	// receives n and "go" (exactly once each: none sent twice on replay).
	if want := 2*pairs*n + 2*pairs; received != want || sent != want {
		t.Fatalf("sent %d, received %d; want %d each", sent, received, want)
	}
}
