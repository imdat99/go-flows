// Command order runs a saga on Postgres: reserve stock, charge, ship. When
// shipping fails, the engine undoes the charge and the reservation (latest
// first), from the workflow's history:
//
//	make up
//	go run ./examples/order
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
	SKU    string `json:"sku"`
	Amount int64  `json:"amount"`
	Ship   string `json:"ship"` // destination; "nowhere" cannot be shipped to
}

var errUnshippable = errors.New("destination not served")

var (
	// Each activity returns its output and the data its compensator needs.
	// The compensator runs (at least once, so idempotent) if the workflow
	// fails or is canceled after the activity succeeded, or may have.
	reserve = flows.DefineActivity("order.reserve",
		func(_ context.Context, k flows.Key, o Order) (string, string, error) {
			id := "res_" + o.ID
			fmt.Printf("  reserve %s (key %s)\n", o.SKU, k)
			return id, id, nil
		},
		func(_ context.Context, _ flows.Key, reservation string) error {
			fmt.Printf("  release %s\n", reservation)
			return nil
		})

	charge = flows.DefineActivity("order.charge",
		func(_ context.Context, _ flows.Key, o Order) (string, string, error) {
			id := fmt.Sprintf("ch_%s_%d", o.ID, o.Amount)
			fmt.Printf("  charge %d\n", o.Amount)
			return id, id, nil
		},
		func(ctx context.Context, k flows.Key, chargeID string) error {
			if flows.CompensationUncertain(ctx) {
				// The charge may or may not exist (an attempt timed out),
				// and chargeID may be empty: refund by the activity's
				// idempotency key, which the payment gateway was given.
				fmt.Printf("  refund by key %s (if charged)\n", k)
				return nil
			}
			fmt.Printf("  refund %s\n", chargeID)
			return nil
		},
		flows.WithTimeout(10*time.Second), // a timed-out charge is uncertain: refunded anyway
		flows.WithCompensateRetry(flows.Exponential(10, 100*time.Millisecond, 5*time.Second)))

	ship = flows.DefineActivity("order.ship",
		func(_ context.Context, _ flows.Key, o Order) (string, struct{}, error) {
			if o.Ship == "nowhere" {
				return "", struct{}{}, flows.Permanent(errUnshippable)
			}
			fmt.Printf("  ship to %s\n", o.Ship)
			return "shp_" + o.ID, struct{}{}, nil
		}, nil)

	placeOrder = flows.DefineWorkflow("order.place", 1, func(c flows.Ctx, o Order) (string, error) {
		// One order per SKU at a time (released when the execution ends).
		if err := flows.Lock(c, "sku:"+o.SKU, 30*time.Second,
			flows.LockWait(flows.Exponential(20, 50*time.Millisecond, time.Second))); err != nil {
			return "", err
		}
		if _, err := reserve.Run(c, o); err != nil {
			return "", err
		}
		if _, err := charge.Run(c, o); err != nil {
			return "", err
		}
		return ship.Run(c, o) // failing here compensates charge, then reserve
	})
)

func init() { flows.RegisterErrorKind("order_unshippable", errUnshippable) }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
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

	suffix := time.Now().Format("150405.000")
	fmt.Println("order 1:")
	out, err := placeOrder.Run(ctx, engine, "order-ok-"+suffix, Order{ID: "1", SKU: "book", Amount: 25, Ship: "Hanoi"})
	if err != nil {
		return err
	}
	fmt.Println("  ->", out)

	fmt.Println("order 2:")
	_, err = placeOrder.Run(ctx, engine, "order-bad-"+suffix, Order{ID: "2", SKU: "book", Amount: 40, Ship: "nowhere"})
	var re *flows.RunError
	switch {
	case errors.As(err, &re) && re.Status == flows.StatusCompensated && errors.Is(err, errUnshippable):
		fmt.Println("  -> compensated:", err)
	default:
		return fmt.Errorf("order 2 should have been compensated, got %w", err)
	}
	return nil
}
