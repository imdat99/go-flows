package flows_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
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

var (
	undone   sync.Map // instance id -> *[]string (guarded by undoneMu)
	undoneMu sync.Mutex

	reserve = flows.DefineActivity("pgtest.reserve", func(_ context.Context, _ flows.Key, sku string) (string, string, error) {
		return "r-" + sku, sku, nil
	}, func(_ context.Context, k flows.Key, sku string) error {
		id, _, _ := strings.Cut(string(k), ":")
		undoneMu.Lock()
		defer undoneMu.Unlock()
		v, _ := undone.LoadOrStore(id, &[]string{})
		*v.(*[]string) = append(*v.(*[]string), sku)
		return nil
	})
	reserveAll = flows.DefineWorkflow("pgtest.reserve-all", 1, func(c flows.Ctx, skus []string) (int, error) {
		for _, s := range skus {
			if _, err := reserve.Run(c, s); err != nil {
				return 0, err
			}
		}
		return 0, errors.New("payment declined")
	})
)

// Compensation on Postgres, with eager execution and with polled tasks.
func TestPostgresCompensation(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, eager := range []bool{true, false} {
		e, err := flows.New(pool, flows.Options{InstanceID: "pg-comp-" + strconv.FormatBool(eager), PollMax: 50 * time.Millisecond, DisableEager: !eager})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		id := "comp-" + strconv.FormatBool(eager)
		undone.Delete(id) // -count reruns
		_, err = reserveAll.Run(wctx, e, id, []string{"a", "b", "c"})
		cancel()
		var re *flows.RunError
		if !errors.As(err, &re) || re.Status != flows.StatusCompensated || !errors.Is(err, flows.ErrCompensated) {
			t.Fatalf("eager=%v: %v", eager, err)
		}
		undoneMu.Lock()
		v, _ := undone.Load(id)
		got := slices.Clone(*v.(*[]string))
		undoneMu.Unlock()
		if !slices.Equal(got, []string{"c", "b", "a"}) {
			t.Fatalf("eager=%v: undone %v", eager, got)
		}
		_ = e.Stop(ctx)
	}
}
