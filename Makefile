.PHONY: up down test race race10 chaos test-chaos lint lint-fix fmt bench bench-goworkflows benchcmp benchgate

BENCH ?= .
BENCHPKG ?= ./...
BENCHOUT ?= bench.txt

up:
	docker compose up -d --wait

down:
	docker compose down -v

# Integration tests use the Postgres of docker compose (make up), or FLOWS_TEST_DSN.
test:
	go test -race -count=1 ./...
	cd analyzer && go test -count=1 ./...
	cd otelflows && go test -race -count=1 ./...

race: test

# Flakiness check before ticking a task.
race10:
	go test -race -count=10 -timeout 90m ./...

# Multi-process chaos tests (build tag chaos), from task T8.1 on.
chaos:
	@if [ -d test/chaos ]; then go test -race -count=1 -tags chaos ./test/chaos/...; \
	else echo "chaos: test/chaos does not exist yet (task T8.1)"; fi

test-chaos: chaos

lint:
	golangci-lint run ./...
	cd analyzer && golangci-lint run --config ../.golangci.yml ./...
	cd otelflows && golangci-lint run --config ../.golangci.yml ./...
	cd bench/goworkflows && golangci-lint run --config ../../.golangci.yml ./...

lint-fix:
	golangci-lint run --fix ./...
	cd analyzer && golangci-lint run --fix --config ../.golangci.yml ./...
	cd otelflows && golangci-lint run --fix --config ../.golangci.yml ./...

fmt:
	golangci-lint fmt ./...
	cd analyzer && golangci-lint fmt --config ../.golangci.yml ./...
	cd otelflows && golangci-lint fmt --config ../.golangci.yml ./...

# Benchmarks: no -race, 6 samples for benchstat.
# The end-to-end benchmarks (a workflow on Postgres per op) run a fixed 50
# iterations per sample: with the default ~7, a few timed polls of the
# pollers swing their per-op allocations; the plan's target is for a warm,
# steady engine.
E2E = ActivityRun|ActivityRun20|PublicActivityRun
# BenchmarkEagerMisses (T10.21: an op is a batch of concurrent workflows on
# 1 or 3 engines, ~0.2-0.4 s) runs a fixed 20 batches per sample: with the
# default timing a sample was 3 to 6 batches, its miss rate mostly warm-up.
EAGER = EagerMisses
# BENCH=... selects benchmarks and runs them with the default timing.
bench:
ifeq ($(BENCH),.)
	go test -run "^$$" -bench . -skip "$(E2E)|$(EAGER)" -benchmem -count 6 $(BENCHPKG) | tee $(BENCHOUT)
	go test -run "^$$" -bench "$(E2E)" -benchtime 50x -benchmem -count 6 $(BENCHPKG) | tee -a $(BENCHOUT)
	go test -run "^$$" -bench "$(EAGER)" -benchtime 20x -benchmem -count 6 $(BENCHPKG) | tee -a $(BENCHOUT)
else
	go test -run "^$$" -bench "$(BENCH)" -benchmem -count 6 $(BENCHPKG) | tee $(BENCHOUT)
endif

# go-workflows on the same scenario (docs/benchmarks.md); a separate module.
bench-goworkflows:
	cd bench/goworkflows && go test -run "^$$" -bench . -benchmem -benchtime 10x -count 3 .

# Compare two bench outputs (make benchcmp OLD=old.txt NEW=bench.txt).
benchcmp:
	@test -n "$(OLD)" -a -n "$(NEW)" || { echo "usage: make benchcmp OLD=old.txt NEW=new.txt"; exit 1; }
	benchstat $(OLD) $(NEW)

# Perf gate: medians of $(BENCHOUT) against the budget in perf-budget.txt.
benchgate:
	go run ./internal/tools/benchgate -budget perf-budget.txt $(BENCHOUT)
