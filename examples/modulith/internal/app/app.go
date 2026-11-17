// Package app is the composition root of the modular monolith: it migrates
// the database, constructs the domain modules' services, builds the
// engine, wires the services into the process layer, starts the background
// work of the modules this process runs, and serves the HTTP API.
//
// One binary, every module and every process compiled in. Config.Modules
// picks the modules whose queues this process serves: all of them in a
// single deployment, a subset per deployment once the monolith is split.
// Nothing else changes: every process registers every definition (they are
// package level), migrates every schema, configures every process package
// and installs the same hook, and work reaches the processes serving its
// queue through Postgres.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/platform/outbox"
	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/modules/notifications"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/modules/payments"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/examples/modulith/processes/fulfilment"
	"github.com/imdat99/go-flows/examples/modulith/processes/notify"
	"github.com/imdat99/go-flows/examples/modulith/processes/payment"
	"github.com/imdat99/go-flows/examples/modulith/processes/replenishment"
	"github.com/imdat99/go-flows/examples/modulith/processes/reservation"
	"github.com/imdat99/go-flows/examples/modulith/processes/shipment"
	"github.com/imdat99/go-flows/flows"
)

// module is what the composition root knows of a module: its migrations,
// and the queue of the processes that work on its tables (a module's
// processes share its queue, so serving a module serves all of them).
type module struct {
	name    string
	queue   string
	migrate func(context.Context, *pgxpool.Pool) error
}

// modules lists every module, in migration order.
var modules = []module{
	{"orders", fulfilment.Queue, orders.Migrate},
	{"payments", payment.Queue, payments.Migrate},
	{"inventory", reservation.Queue, inventory.Migrate}, // replenishment.Queue too
	{"shipping", shipment.Queue, shipping.Migrate},
	{"notifications", notify.Queue, notifications.Migrate},
}

// services are the domain modules' services, constructed once per process.
type services struct {
	orders        *orders.Service
	payments      *payments.Service
	inventory     *inventory.Service
	shipping      *shipping.Service
	notifications *notifications.Service
}

func newServices(pool *pgxpool.Pool, log *slog.Logger) (services, error) {
	var s services
	var errs [5]error
	s.orders, errs[0] = orders.NewService(pool, log)
	s.payments, errs[1] = payments.NewService(pool, log)
	s.inventory, errs[2] = inventory.NewService(pool, log)
	s.shipping, errs[3] = shipping.NewService(pool, log)
	s.notifications, errs[4] = notifications.NewService(pool, log)
	return s, errors.Join(errs[:]...)
}

// ModuleNames returns the names of every module.
func ModuleNames() []string {
	names := make([]string, len(modules))
	for i, m := range modules {
		names[i] = m.name
	}
	return names
}

// Config configures an App.
type Config struct {
	// Modules this process runs (their queues and background work). Empty:
	// none, the process only serves the API (a client-only engine).
	Modules []string
	// EngineID names the engine in leases; unique per process (default:
	// hostname plus a random suffix).
	EngineID string
	Logger   *slog.Logger
	// Observer, if set, also receives the engine's measurements.
	Observer flows.Observer
	// PickupWithin is how long orders wait for the carrier (default 48h).
	PickupWithin time.Duration
	// ReplenishEvery is the period of the stock replenishment loops
	// (default 1m).
	ReplenishEvery time.Duration

	// PurgeEvery is the period of the retention job (purgeOnce): ended
	// workflows and old inventory request records are deleted. 0: this
	// process does not run it. One process is enough; concurrent runs are
	// safe, only wasted work.
	PurgeEvery time.Duration
	// Retention is how long ended workflows are kept (0 or less: the
	// default, 30 days; to keep them forever, set PurgeEvery to 0).
	Retention time.Duration
	// KeepPurgedIDs is how long an instance id stays used after its
	// workflow was purged (flows.Options.KeepPurgedIDs; default 90 days,
	// negative: none). Every process gets the same value. Starting a purged
	// id fails with flows.ErrInstancePurged: an order or a request id
	// replayed after retention is not run again.
	KeepPurgedIDs time.Duration
}

// Defaults of the retention settings.
const (
	defaultRetention     = 30 * 24 * time.Hour
	defaultKeepPurgedIDs = 90 * 24 * time.Hour
	// purgeBatch bounds each delete of the retention job.
	purgeBatch = 1000
)

// App is one process of the monolith.
type App struct {
	cfg    Config
	pool   *pgxpool.Pool
	engine *flows.Engine
	log    *slog.Logger
	svc    services

	started bool
	stopBg  context.CancelFunc // stops the relay and the retention job
	bg      sync.WaitGroup
}

// New migrates the database, constructs the modules' services, builds the
// engine and configures the process layer.
// Call Start next.
func New(ctx context.Context, pool *pgxpool.Pool, cfg Config) (*App, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultRetention
	}
	switch {
	case cfg.KeepPurgedIDs == 0:
		cfg.KeepPurgedIDs = defaultKeepPurgedIDs
	case cfg.KeepPurgedIDs < 0:
		cfg.KeepPurgedIDs = 0
	}
	queues := make([]string, 0, len(cfg.Modules))
	for _, name := range cfg.Modules {
		i := slices.IndexFunc(modules, func(m module) bool { return m.name == name })
		if i < 0 {
			return nil, fmt.Errorf("unknown module %q (known: %v)", name, ModuleNames())
		}
		queues = append(queues, modules[i].queue)
	}

	// Every process migrates every schema: the API and the hook use tables
	// of modules the process may not run. Migrations are serialized by
	// advisory locks, so processes starting together are safe.
	if err := flows.Migrate(ctx, pool); err != nil {
		return nil, fmt.Errorf("migrate flows: %w", err)
	}
	if err := outbox.Migrate(ctx, pool); err != nil {
		return nil, err
	}
	for _, m := range modules {
		if err := m.migrate(ctx, pool); err != nil {
			return nil, err
		}
	}

	svc, err := newServices(pool, log)
	if err != nil {
		return nil, err
	}

	var obs flows.Observer = logObserver{log}
	if cfg.Observer != nil {
		obs = observers{obs, cfg.Observer}
	}
	engine, err := flows.New(pool, flows.Options{
		InstanceID: cfg.EngineID,
		Queues:     queues, // empty: the default queue, which no module uses
		Logger:     log,
		// The same hooks on every engine: whichever engine records an end
		// (or performs a Terminate) runs them.
		Hook:     runHooks{fulfilment.Hook(svc.orders)},
		Observer: obs,
		// Instance ids stay idempotency keys past retention (Place,
		// Adjust, the notify consumer). Set on every engine, client-only
		// ones included: they start workflows too.
		KeepPurgedIDs: cfg.KeepPurgedIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("new engine: %w", err)
	}

	// The module services reach the activities here: the one place the
	// process layer's package-level holders are set (definitions are
	// global, see processes/internal/adapt). Every process package is
	// configured, its module run here or not: the HTTP API of any process
	// starts workflows (Place, Adjust). A wiring mistake fails here, at
	// startup, rather than as activities retrying adapt.ErrNotConfigured on
	// a live replica.
	err = errors.Join(
		fulfilment.Configure(fulfilment.Services{Orders: svc.orders, PickupWithin: cfg.PickupWithin}),
		payment.Configure(payment.Services{Payments: svc.payments}),
		reservation.Configure(reservation.Services{Inventory: svc.inventory}),
		replenishment.Configure(replenishment.Services{Inventory: svc.inventory, Every: cfg.ReplenishEvery}),
		shipment.Configure(shipment.Services{Shipping: svc.shipping}),
		notify.Configure(notify.Services{Notifications: svc.notifications, Logger: log}),
	)
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, pool: pool, engine: engine, log: log, svc: svc}, nil
}

// Engine returns the process's engine.
func (a *App) Engine() *flows.Engine { return a.engine }

func (a *App) runs(name string) bool { return slices.Contains(a.cfg.Modules, name) }

// Start starts the engine and the background work of the modules this
// process runs. If it fails, call Stop: the engine may have started.
func (a *App) Start(ctx context.Context) error {
	if len(a.cfg.Modules) > 0 {
		if err := a.engine.Start(ctx); err != nil {
			return fmt.Errorf("start engine: %w", err)
		}
		a.started = true
	}
	if a.runs("inventory") {
		if err := replenishment.Start(ctx, a.engine); err != nil {
			return err
		}
	}
	// Background work lives until Stop, not until ctx.
	bgCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	a.stopBg = stop
	if a.runs("notifications") {
		// With a real broker the relay is a role of its own (any process)
		// and the consumers subscribe to the broker. The in-process bus
		// ties them to the process running the notifications module.
		bus := &outbox.LocalBus{}
		notify.Subscribe(bus, a.engine)
		relay := &outbox.Relay{Pool: a.pool, Bus: bus, Logger: a.log}
		a.bg.Go(func() { relay.Run(bgCtx) })
	}
	if a.cfg.PurgeEvery > 0 {
		a.bg.Go(func() { a.purgeLoop(bgCtx) })
	}
	a.log.LogAttrs(ctx, slog.LevelInfo, "modulith: started", slog.Any("modules", a.cfg.Modules))
	return nil
}

// Stop stops the relay and the retention job, then drains the engine until
// ctx is done (running tasks are then canceled, and resumed by another
// process).
func (a *App) Stop(ctx context.Context) error {
	if a.stopBg != nil {
		a.stopBg()
		a.bg.Wait()
	}
	if !a.started {
		return nil
	}
	if err := a.engine.Stop(ctx); err != nil {
		return fmt.Errorf("stop engine: %w", err)
	}
	return nil
}

// purgeLoop runs purgeOnce every PurgeEvery until ctx is done. A failed run
// is logged and retried at the next tick.
func (a *App) purgeLoop(ctx context.Context) {
	t := time.NewTicker(a.cfg.PurgeEvery)
	defer t.Stop()
	for {
		if err := a.purgeOnce(ctx); err != nil && ctx.Err() == nil {
			a.log.LogAttrs(ctx, slog.LevelWarn, "modulith: retention job failed", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// purgeOnce is the retention job, in batches of purgeBatch:
//
//   - the workflows ended more than Retention ago (Engine.PurgeEnded); with
//     KeepPurgedIDs their instance ids stay used, as tombstones, which the
//     same call expires;
//   - the inventory module's records of processed requests older than
//     Retention plus KeepPurgedIDs: an activity repeats its key only while
//     its workflow runs (a workflow parked for longer than that must be
//     resolved first: alert on needs_attention).
//
// The modules' own tables (orders, payments, shipments, adjustments) are
// kept: they are the lasting idempotency of their ids. Old sent outbox rows
// are not deleted here (see README, "Operations checklist").
func (a *App) purgeOnce(ctx context.Context) error {
	var runs, requests int64
	for {
		n, err := a.engine.PurgeEnded(ctx, a.cfg.Retention, purgeBatch)
		if err != nil {
			return fmt.Errorf("purge ended workflows: %w", err)
		}
		runs += int64(n)
		if n < purgeBatch {
			break
		}
	}
	for {
		n, err := a.svc.inventory.PurgeRequests(ctx, a.cfg.Retention+a.cfg.KeepPurgedIDs, purgeBatch)
		if err != nil {
			return err
		}
		requests += n
		if n < purgeBatch {
			break
		}
	}
	if runs+requests > 0 {
		a.log.LogAttrs(ctx, slog.LevelInfo, "modulith: retention job",
			slog.Int64("workflows", runs), slog.Int64("inventory_requests", requests))
	}
	return nil
}

// runHooks runs several modules' hooks in the same transaction.
type runHooks []flows.RunHook

func (hs runHooks) RunEnded(ctx context.Context, tx pgx.Tx, r flows.RunInfo) error {
	for _, h := range hs {
		if err := h.RunEnded(ctx, tx, r); err != nil {
			return err
		}
	}
	return nil
}

// logObserver logs ends and failed attempts. Observers run on the engine's
// path: keep them fast (logging, counters).
type logObserver struct{ log *slog.Logger }

func (o logObserver) ActivityAttempt(a flows.ActivityAttempt) {
	if a.Err == nil {
		return
	}
	o.log.LogAttrs(context.Background(), slog.LevelWarn, "modulith: activity attempt failed",
		slog.String("instance_id", a.InstanceID), slog.String("activity", a.Activity),
		slog.Int("attempt", a.Attempt), slog.Bool("compensation", a.Compensation),
		slog.String("outcome", a.Outcome), slog.Any("error", a.Err))
}

func (o logObserver) RunEnded(r flows.RunInfo) {
	level := slog.LevelInfo
	if r.Status == flows.StatusNeedsAttention {
		level = slog.LevelError // alert: an operator must act
	}
	o.log.LogAttrs(context.Background(), level, "modulith: workflow ended",
		slog.String("instance_id", r.InstanceID), slog.String("workflow", r.Workflow),
		slog.Int("version", r.Version), slog.String("status", string(r.Status)), slog.String("error", r.Error))
}

type observers []flows.Observer

func (os observers) ActivityAttempt(a flows.ActivityAttempt) {
	for _, o := range os {
		o.ActivityAttempt(a)
	}
}

func (os observers) RunEnded(r flows.RunInfo) {
	for _, o := range os {
		o.RunEnded(r)
	}
}
