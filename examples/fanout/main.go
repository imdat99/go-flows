// Command fanout runs a workflow that resizes images in parallel, reports
// progress through Select, and stops early on a "cancel" signal:
//
//	make up
//	go run ./examples/fanout
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows"
)

var (
	resize = flows.DefineActivity("resize", func(_ context.Context, _ flows.Key, img string) (string, struct{}, error) {
		time.Sleep(20 * time.Millisecond)
		return img + "@512px", struct{}{}, nil
	}, nil)

	album = flows.DefineWorkflow("resize-album", 1, func(c flows.Ctx, images []string) ([]string, error) {
		done := make([]*flows.Future[string], len(images))
		for i, img := range images {
			done[i] = resize.Start(c, img) // all in parallel
		}
		stop := flows.Signals[string](c, "cancel")
		out := make([]string, 0, len(images))
		for _, f := range done {
			canceled := false
			flows.Select(c,
				flows.OnFuture(f, func(_ flows.Ctx, v string, err error) {
					if err == nil {
						out = append(out, v)
					}
				}),
				flows.OnSignal(stop, func(flows.Ctx, string, error) { canceled = true }),
			)
			if canceled {
				break
			}
		}
		return out, nil
	})
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	dsn := os.Getenv("FLOWS_DSN")
	if dsn == "" {
		dsn = "postgres://flows:flows@localhost:5433/flows?sslmode=disable" //nolint:gosec // the docker compose database of the repository
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := flows.Migrate(ctx, pool); err != nil {
		return err
	}
	engine, err := flows.New(pool, flows.Options{})
	if err != nil {
		return err
	}
	if err := engine.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = engine.Stop(context.Background()) }()
	id := fmt.Sprintf("album-%d", time.Now().UnixNano())
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := album.Run(wctx, engine, id, []string{"a.png", "b.png", "c.png", "d.png"})
	if err != nil {
		return err
	}
	fmt.Println(id, out)
	return nil
}
