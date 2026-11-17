// Command modulith runs the modular monolith example: all modules in one
// process, or some of them per process.
//
//	make up
//	go run ./examples/modulith/cmd/modulith                       # every module, API on :8080
//	go run ./examples/modulith/cmd/modulith -modules=orders,notifications -http=:8080
//	go run ./examples/modulith/cmd/modulith -modules=payments,inventory,shipping -http=
//
// Flags fall back to the environment: MODULITH_MODULES, MODULITH_HTTP,
// MODULITH_ID, MODULITH_PURGE_EVERY, MODULITH_RETENTION,
// MODULITH_KEEP_PURGED_IDS, FLOWS_DSN. See examples/modulith/README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/internal/app"
)

// shutdownTimeout bounds the drain at shutdown: running tasks still
// running then are canceled and resumed by another process.
const shutdownTimeout = 30 * time.Second

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.LogAttrs(context.Background(), slog.LevelError, "modulith: exit", slog.Any("error", err))
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func run(log *slog.Logger) error {
	modules := flag.String("modules", env("MODULITH_MODULES", strings.Join(app.ModuleNames(), ",")),
		"comma-separated modules this process runs (empty: API only)")
	addr := flag.String("http", env("MODULITH_HTTP", ":8080"), "HTTP API address (empty: no API)")
	engineID := flag.String("id", env("MODULITH_ID", ""), "engine instance id, unique per process (default: hostname + random)")
	purgeEvery, err := envDuration("MODULITH_PURGE_EVERY", time.Hour)
	if err != nil {
		return err
	}
	retention, err := envDuration("MODULITH_RETENTION", 30*24*time.Hour)
	if err != nil {
		return err
	}
	keepIDs, err := envDuration("MODULITH_KEEP_PURGED_IDS", 90*24*time.Hour)
	if err != nil {
		return err
	}
	flag.DurationVar(&purgeEvery, "purge-every", purgeEvery,
		"period of the retention job (0: not in this process; one process is enough)")
	flag.DurationVar(&retention, "retention", retention, "how long ended workflows are kept")
	flag.DurationVar(&keepIDs, "keep-purged-ids", keepIDs,
		"how long a purged instance id stays used (negative: none); the same on every process")
	flag.Parse()
	dsn := env("FLOWS_DSN", "postgres://flows:flows@localhost:5433/flows?sslmode=disable") // the docker compose database of the repository

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	var names []string
	for n := range strings.SplitSeq(*modules, ",") {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, n)
		}
	}
	a, err := app.New(ctx, pool, app.Config{
		Modules: names, EngineID: *engineID, Logger: log,
		PurgeEvery: purgeEvery, Retention: retention, KeepPurgedIDs: keepIDs,
	})
	if err != nil {
		return err
	}
	if err := a.Start(ctx); err != nil {
		// The engine may have started before the failure: drain it.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		return errors.Join(err, a.Stop(sctx))
	}

	var srv *http.Server
	srvErr := make(chan error, 1)
	if *addr != "" {
		srv = &http.Server{
			Addr: *addr, Handler: a.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second, // above the API's longest wait (adjustWait)
			IdleTimeout:       time.Minute,
		}
		go func() { srvErr <- srv.ListenAndServe() }()
		log.LogAttrs(ctx, slog.LevelInfo, "modulith: listening", slog.String("addr", *addr))
	}

	select {
	case <-ctx.Done():
	case err := <-srvErr:
		log.LogAttrs(ctx, slog.LevelError, "modulith: http server", slog.Any("error", err))
	}

	// Graceful shutdown: stop taking requests, then drain the engine.
	// context.WithoutCancel: ctx is already canceled by the signal.
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	var errs []error
	if srv != nil {
		if err := srv.Shutdown(sctx); err != nil {
			errs = append(errs, fmt.Errorf("http shutdown: %w", err))
		}
	}
	if err := a.Stop(sctx); err != nil {
		errs = append(errs, err)
	}
	log.LogAttrs(sctx, slog.LevelInfo, "modulith: stopped")
	return errors.Join(errs...)
}
