package testdb_test

import (
	"context"
	"testing"

	"github.com/imdat99/go-flows/internal/testdb"
)

func TestNewGivesIsolatedDatabases(t *testing.T) {
	t.Parallel()
	a, b := testdb.New(t), testdb.New(t)
	ctx := context.Background()
	if _, err := a.Exec(ctx, "CREATE TABLE only_in_a (id int)"); err != nil {
		t.Fatal(err)
	}
	var n int
	err := b.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE tablename = 'only_in_a'").Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("table created in one test database is visible in another")
	}
	if first, second := testdb.UniqueName(t, "x"), testdb.UniqueName(t, "x"); first == second {
		t.Fatal("UniqueName returned the same name twice")
	}
}
