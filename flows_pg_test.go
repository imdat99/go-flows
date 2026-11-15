package flows_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

type item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

var (
	pick = flows.DefineActivity("pgtest.pick", func(_ context.Context, k flows.Key, it item) (string, string, error) {
		return string(k), it.SKU, nil
	}, nil)
	pickAll = flows.DefineWorkflow("pgtest.pick-all", 1, func(c flows.Ctx, items []item) ([]string, error) {
		futures := make([]*flows.Future[string], len(items))
		for i, it := range items {
			futures[i] = pick.Start(c, it) // fan out
		}
		keys := make([]string, len(items))
		for i, f := range futures {
			k, err := f.Get(c)
			if err != nil {
				return nil, err
			}
			keys[i] = k
		}
		if err := flows.Sleep(c, 20*time.Millisecond); err != nil {
			return nil, err
		}
		return keys, nil
	})
)

// End to end on Postgres through the public API, with two engines sharing
// the database.
func TestPostgresEndToEnd(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-" + strconv.Itoa(i), PollMax: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Stop(ctx) })
	}
	client, err := flows.New(pool, flows.Options{InstanceID: "client"}) // not started: client only
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	handles := make([]*flows.Handle[[]string], 10)
	for i := range handles {
		h, err := pickAll.Start(ctx, client, "pick-"+strconv.Itoa(i), []item{{"a", 1}, {"b", 2}, {"c", 3}})
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}
	for i, h := range handles {
		keys, err := h.Wait(wctx)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		want := "pick-" + strconv.Itoa(i) + ":1:pgtest.pick"
		if len(keys) != 3 || keys[0] != want {
			t.Fatalf("run %d keys %v, want first %q", i, keys, want)
		}
	}
}
