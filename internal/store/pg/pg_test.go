package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/store/pg"
	"github.com/imdat99/go-flows/internal/store/storetest"
	"github.com/imdat99/go-flows/internal/testdb"
)

func newStore(t *testing.T) *pg.Store {
	t.Helper()
	return newStoreB(t)
}

func newStoreB(tb testing.TB) *pg.Store {
	tb.Helper()
	pool := testdb.New(tb)
	if err := pg.Migrate(context.Background(), pool); err != nil {
		tb.Fatal(err)
	}
	return pg.New(pool)
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Harness{
		New: func(t *testing.T) store.Store {
			t.Helper()
			return newStore(t)
		},
		Advance: func(t *testing.T, _ store.Store, d time.Duration) {
			t.Helper()
			time.Sleep(d)
		},
		TTL: time.Second, // generous: 40 parallel cases share one Postgres
	})
}
