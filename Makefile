.PHONY: up down test race race10 chaos test-chaos lint lint-fix fmt bench benchcmp benchgate

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

race: test

# Flakiness check before ticking a task.
race10:
	go test -race -count=10 ./...

# Multi-process chaos tests (build tag chaos), from task T8.1 on.
chaos:
	@if [ -d test/chaos ]; then go test -race -count=1 -tags chaos ./test/chaos/...; \
	else echo "chaos: test/chaos does not exist yet (task T8.1)"; fi

test-chaos: chaos

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

fmt:
	golangci-lint fmt ./...

# Benchmarks: no -race, 6 samples for benchstat.
bench:
	go test -run "^$$" -bench "$(BENCH)" -benchmem -count 6 $(BENCHPKG) | tee $(BENCHOUT)

# Compare two bench outputs (make benchcmp OLD=old.txt NEW=bench.txt).
benchcmp:
	@test -n "$(OLD)" -a -n "$(NEW)" || { echo "usage: make benchcmp OLD=old.txt NEW=new.txt"; exit 1; }
	benchstat $(OLD) $(NEW)

# Perf gate: medians of $(BENCHOUT) against the budget in perf-budget.txt.
benchgate:
	go run ./internal/tools/benchgate -budget perf-budget.txt $(BENCHOUT)
