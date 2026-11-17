//go:build chaos

package chaos_test

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- I: database-side faults (T11.7) ----

// soakTTL is the children's LeaseTTL in the database-side tests.
const soakTTL = 3 * time.Second

// I1: the database blackholes under an engine (its connections freeze,
// nothing is closed) while its activities run, for longer than its lease.
// The frozen engine gives its leases up: its attempts are canceled with
// ErrLeaseLost before another engine takes them over once the leases
// expired (no two attempts of an activity overlap) (T11.1); Stop on the
// still frozen engine returns within its deadline (T11.2); after the thaw
// every workflow completes as it should.
func TestDBBlackhole(t *testing.T) {
	cl := newCluster(t, 2, soakTTL, envSlow+"=1") // soak.step outlasts the lease
	cl.startOrders(10)
	a, b := cl.slots[0], cl.slots[1]
	frozenProc := a.proc
	pid := frozenProc.pid()
	waitFor(t, "the engine to blackhole to run activities", 2*time.Minute, func() bool {
		return count(t, cl.check, `SELECT count(*) FROM soak_attempt WHERE pid = $1 AND step = 'soak.step' AND ended IS NULL`, pid) >= 2
	})
	a.proxy.Freeze()
	frozen := time.Now()
	var running []int64
	if err := cl.check.QueryRow(context.Background(), `
		SELECT coalesce(array_agg(id), '{}') FROM soak_attempt WHERE pid = $1 AND ended IS NULL`, pid).Scan(&running); err != nil {
		t.Fatal(err)
	}
	// Given up within the lease (ttl - ttl/3 after the last renewal): every
	// attempt running at the freeze ends, those still in their work
	// canceled with ErrLeaseLost.
	waitFor(t, "the frozen engine's attempts to end", 2*soakTTL+5*time.Second, func() bool {
		return count(t, cl.check, `SELECT count(*) FROM soak_attempt WHERE id = ANY($1) AND ended IS NULL`, running) == 0
	})
	gaveUp := time.Since(frozen)
	lost := count(t, cl.check, `SELECT count(*) FROM soak_attempt WHERE id = ANY($1) AND outcome = 'lease-lost'`, running)
	if lost == 0 {
		t.Fatalf("none of the %d attempts running at the freeze was canceled with ErrLeaseLost", len(running))
	}
	// The other engine takes them over (once the leases expired).
	waitFor(t, "the other engine to take an activity over", time.Minute, func() bool {
		return count(t, cl.check, `
			SELECT count(*) FROM soak_attempt a JOIN soak_attempt l ON l.key = a.key
			WHERE l.id = ANY($1) AND l.outcome = 'lease-lost' AND a.pid = $2`, running, b.proc.pid()) > 0
	})
	// T11.3: the lost attempt counted toward the retry policy, the
	// takeover's attempt number is higher.
	var lostAt, takeoverAt int
	if err := cl.check.QueryRow(context.Background(), `
		SELECT l.attempt, a.attempt FROM soak_attempt a JOIN soak_attempt l ON l.key = a.key
		WHERE l.id = ANY($1) AND l.outcome = 'lease-lost' AND a.pid = $2
		ORDER BY a.started LIMIT 1`, running, b.proc.pid()).Scan(&lostAt, &takeoverAt); err != nil {
		t.Fatal(err)
	}
	if lostAt < 1 || takeoverAt <= lostAt {
		t.Errorf("lost attempt %d taken over as attempt %d: the lost attempt was not counted", lostAt, takeoverAt)
	}
	if !strings.Contains(frozenProc.out.String(), "LOST activity") {
		t.Error("the frozen engine reported no activity attempt ended with ErrLeaseLost")
	}
	took := cl.frozenStop(a, 0) // still frozen
	t.Logf("attempts running at the freeze: %d, canceled with ErrLeaseLost: %d, all ended after %v; lost attempt %d taken over as attempt %d; Stop of the frozen engine took %v",
		len(running), lost, gaveUp.Round(time.Millisecond), lostAt, takeoverAt, took)
	cl.waitEnded(4 * time.Minute)
	cl.verify()
}

// I2: every backend of the database is terminated (pg_terminate_backend)
// again and again while the engines run: every workflow still completes
// as it should, once.
func TestDBConnectionCuts(t *testing.T) {
	cl := newCluster(t, 3, soakTTL)
	cl.startOrders(20)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var cuts, backends int
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(300*time.Millisecond + rand.N(400*time.Millisecond)):
			}
			backends += cl.terminate()
			cuts++
		}
	})
	cl.waitEnded(5 * time.Minute)
	close(stop)
	wg.Wait()
	t.Logf("%d rounds of terminations, %d backends terminated", cuts, backends)
	if cuts < 5 {
		t.Fatalf("only %d rounds of terminations: the workload ended before the faults", cuts)
	}
	cl.verify()
}

// I3: a DBA sets a low statement_timeout (5ms, or CHAOS_STATEMENT_TIMEOUT)
// on the database (ALTER DATABASE, the pools reconnecting with it) while
// the engines run: statements time out (every kind now and then, claims
// mostly), and every workflow still completes as it should, retried. (At
// 200ms none times out on an idle machine.)
func TestDBStatementTimeout(t *testing.T) {
	timeout := 5 * time.Millisecond
	if v := os.Getenv("CHAOS_STATEMENT_TIMEOUT"); v != "" {
		var err error
		if timeout, err = time.ParseDuration(v); err != nil || timeout < time.Millisecond {
			t.Fatalf("CHAOS_STATEMENT_TIMEOUT: %q", v)
		}
	}
	cl := newCluster(t, 2, soakTTL)
	cl.setStatementTimeout(timeout)
	probe := cl.pool("chaos-probe", false)
	var st string
	if err := probe.QueryRow(context.Background(), `SHOW statement_timeout`).Scan(&st); err != nil || st != strconv.Itoa(int(timeout.Milliseconds()))+"ms" {
		t.Fatalf("statement_timeout of a new connection: %q, %v", st, err)
	}
	cl.startOrders(30)
	cl.waitEnded(5 * time.Minute)
	cl.verify()
	timeouts := map[string]int{}
	for _, s := range cl.slots {
		s.proc.out.addTimeouts(timeouts)
	}
	t.Logf("statement timeouts logged by the engines, by message: %v", timeouts)
	if len(timeouts) == 0 {
		t.Errorf("no statement timed out at %v: the fault was not exercised", timeout)
	}
}

// ---- J: the soak ----

// TestSoak runs a continuous workload for CHAOS_DURATION (make chaos-soak)
// under a random mix of process faults (SIGKILL, SIGSTOP, Stop on a
// blackholed engine) and database faults (blackholes, connection cuts, a
// low statement_timeout), then checks the integrity of the end state
// (cluster.verify). CHAOS_SEED replays a mix.
func TestSoak(t *testing.T) {
	v := os.Getenv("CHAOS_DURATION")
	if v == "" {
		t.Skip("CHAOS_DURATION not set (make chaos-soak)")
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("CHAOS_DURATION: %v", err)
	}
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		if seed, err = strconv.ParseUint(s, 10, 64); err != nil {
			t.Fatalf("CHAOS_SEED: %v", err)
		}
	}
	t.Logf("CHAOS_DURATION=%v CHAOS_SEED=%d", dur, seed)
	rng := rand.New(rand.NewPCG(seed, seed))
	between := func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rng.Int64N(int64(hi-lo))) }

	cl := newCluster(t, 3, soakTTL, envSoak+"=1")
	const inFlight = 24
	stopLoad := make(chan struct{})
	var load sync.WaitGroup
	load.Go(func() { // keeps inFlight orders open
		for {
			if n, err := cl.openN(); err == nil && n < inFlight {
				cl.startOrders(inFlight - n)
			}
			select {
			case <-stopLoad:
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	})
	start, deadline := time.Now(), time.Now().Add(dur)
	lastReport := start
	for time.Now().Before(deadline) {
		time.Sleep(between(500*time.Millisecond, 3*time.Second))
		cl.parked()
		s := cl.slots[rng.IntN(len(cl.slots))]
		switch rng.IntN(8) {
		case 0:
			cl.kill(s)
		case 1:
			cl.pause(s, between(soakTTL/3, 5*soakTTL/2))
		case 2, 3:
			cl.blackhole(s, between(soakTTL/3, 5*soakTTL/2))
		case 4:
			cl.frozenStop(s, between(0, soakTTL))
		case 5:
			for range 1 + rng.IntN(5) {
				cl.terminate()
				time.Sleep(between(100*time.Millisecond, 600*time.Millisecond))
			}
		case 6:
			cl.setStatementTimeout([]time.Duration{50 * time.Millisecond, 200 * time.Millisecond}[rng.IntN(2)])
			time.Sleep(between(3*time.Second, 15*time.Second))
			cl.setStatementTimeout(0)
		case 7: // two at once: a blackhole and a pause on another process
			o := cl.slots[(s.idx+1)%len(cl.slots)]
			var both sync.WaitGroup
			d1, d2 := between(soakTTL/3, 2*soakTTL), between(soakTTL/3, 2*soakTTL)
			both.Go(func() { cl.blackhole(s, d1) })
			both.Go(func() { cl.pause(o, d2) })
			both.Wait()
		}
		if time.Since(lastReport) >= time.Minute {
			lastReport = time.Now()
			cl.mu.Lock()
			t.Logf("%v: %d orders started, faults %v", time.Since(start).Round(time.Second), len(cl.ids), cl.faults)
			cl.mu.Unlock()
		}
	}
	close(stopLoad)
	load.Wait()
	cl.waitEnded(10 * time.Minute)
	cl.mu.Lock()
	t.Logf("done after %v: %d orders, faults %v", time.Since(start).Round(time.Second), len(cl.ids), cl.faults)
	cl.mu.Unlock()
	cl.verify()
}
