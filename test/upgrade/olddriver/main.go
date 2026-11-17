// Command olddriver is the first half of the upgrade test (test/upgrade):
// built against the release the test upgrades from, it migrates the
// database (FLOWS_UPGRADE_DSN) with that release, starts the executions of
// package defs, waits until each is at its upgrade point, and stops its
// engine cleanly. It prints "OLD: ready" before stopping and exits 0 on
// success.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/test/upgrade/defs"
)

func main() { os.Exit(run()) }

func run() int {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("FLOWS_UPGRADE_DSN"))
	if err != nil {
		fmt.Println("OLD: connect:", err)
		return 2
	}
	defer pool.Close()
	if err := flows.Migrate(ctx, pool); err != nil {
		fmt.Println("OLD: migrate:", err)
		return 2
	}
	defs.Setup(pool, defs.PhaseOld)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	eng, err := flows.New(pool, defs.Options("old", logger))
	if err != nil {
		fmt.Println("OLD: engine:", err)
		return 2
	}
	if err := eng.Start(ctx); err != nil {
		fmt.Println("OLD: start:", err)
		return 2
	}
	code := 0
	if err := defs.StartOld(ctx, eng); err != nil {
		fmt.Println("OLD: start workflows:", err)
		code = 1
	} else if err := waitReady(ctx, pool); err != nil {
		fmt.Println("OLD:", err)
		code = 1
	} else {
		fmt.Println("OLD: ready")
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := eng.Stop(sctx); err != nil {
		fmt.Println("OLD: stop:", err)
		return 1
	}
	fmt.Println("OLD: stopped")
	return code
}

// waitReady polls until every execution is at its upgrade point.
func waitReady(ctx context.Context, pool *pgxpool.Pool) error {
	begin := time.Now()
	deadline := begin.Add(2 * time.Minute)
	last := ""
	for {
		unready, err := defs.Unready(ctx, pool)
		if err != nil {
			return err
		}
		if now := strings.Join(unready, "; "); now != last {
			fmt.Printf("OLD: %v unready: %s\n", time.Since(begin).Round(time.Millisecond), now)
			last = now
		}
		if len(unready) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not at the upgrade point: %s", strings.Join(unready, "; "))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
