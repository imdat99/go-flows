// Command quickstart runs an order workflow on Postgres:
//
//	make up
//	go run ./examples/quickstart
//
// FLOWS_DSN points at another database.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows"
)

type Order struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"`
}

type Receipt struct {
	Charge   string `json:"charge"`
	Shipment string `json:"shipment"`
}

var (
	// Activities do the side effects. Their Key is stable across retries
	// and replays: use it as the idempotency key towards other systems.
	charge = flows.DefineActivity("charge", func(_ context.Context, k flows.Key, o Order) (string, string, error) {
		return fmt.Sprintf("ch_%s_%d", o.ID, o.Amount), string(k), nil
	}, nil, flows.WithRetry(flows.Exponential(5, 100*time.Millisecond, 2*time.Second)))

	ship = flows.DefineActivity("ship", func(_ context.Context, _ flows.Key, id string) (string, struct{}, error) {
		return "shp_" + id, struct{}{}, nil
	}, nil)

	// The workflow is plain Go, replayed deterministically from its
	// history: here it waits for an approval signal, at most a minute.
	process = flows.DefineWorkflow("process-order", 1, func(c flows.Ctx, o Order) (Receipt, error) {
		ch, err := charge.Run(c, o)
		if err != nil {
			return Receipt{}, err
		}
		if _, err := flows.Await[bool](c, "approved", time.Minute); err != nil {
			return Receipt{}, err
		}
		sh, err := ship.Run(c, o.ID)
		return Receipt{Charge: ch, Shipment: sh}, err
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

	id := fmt.Sprintf("order-%d", time.Now().UnixNano())
	h, err := process.Start(ctx, engine, id, Order{ID: id, Amount: 12999})
	if err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	if err := engine.Signal(ctx, id, "approved", true); err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r, err := h.Wait(wctx)
	if re, ok := errors.AsType[*flows.RunError](err); ok {
		return fmt.Errorf("order %s ended %s: %w", id, re.Status, err)
	}
	if err != nil {
		return err
	}
	fmt.Printf("order %s: charged %s, shipped %s\n", id, r.Charge, r.Shipment)
	return nil
}
