package flows_test

import (
	"context"
	"errors"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

var (
	purgedLeaf   = flows.DefineWorkflow("pgtest.purged-leaf", 1, func(_ flows.Ctx, n int) (int, error) { return n + 1, nil })
	purgedParent = flows.DefineWorkflow("pgtest.purged-parent", 1, func(c flows.Ctx, kid string) (bool, error) {
		_, err := purgedLeaf.Child(c, kid, 1).Get(c)
		return errors.Is(err, flows.ErrInstancePurged) && errors.Is(err, flows.ErrInstanceExists), nil
	})
)

// T12.3 on Postgres through the public API: a purged id is refused by Start
// and by a child start, then forgotten and started again; a tombstone
// expires with KeepPurgedIDs.
func TestPostgresKeepPurgedIDs(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	const keep = 1500 * time.Millisecond
	e, err := flows.New(pool, flows.Options{InstanceID: "pg-keep", PollMax: 50 * time.Millisecond, KeepPurgedIDs: keep})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(ctx) })
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, id := range []string{"keep-1", "keep-kid"} {
		if _, err := purgedLeaf.Run(wctx, e, id, 1); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)
	if n, err := e.PurgeEnded(ctx, 0, 100); err != nil || n != 2 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if _, err := purgedLeaf.Start(ctx, e, "keep-1", 1); !errors.Is(err, flows.ErrInstancePurged) {
		t.Fatalf("start of a purged id: %v", err)
	}
	if ok, err := purgedParent.Run(wctx, e, "keep-parent", "keep-kid"); err != nil || !ok {
		t.Fatalf("child with a purged id: %v %v", ok, err)
	}
	if err := e.ForgetInstance(ctx, "keep-1"); err != nil {
		t.Fatal(err)
	}
	if v, err := purgedLeaf.Run(wctx, e, "keep-1", 2); err != nil || v != 3 {
		t.Fatalf("run after forget: %d %v", v, err)
	}
	// The tombstone of keep-kid expires: a purge deletes it.
	time.Sleep(keep + 100*time.Millisecond)
	if _, err := e.PurgeEnded(ctx, time.Hour, 100); err != nil {
		t.Fatal(err)
	}
	if v, err := purgedLeaf.Run(wctx, e, "keep-kid", 5); err != nil || v != 6 {
		t.Fatalf("run after expiry: %d %v", v, err)
	}
}
