package coro

import (
	"sync"
	"time"
)

// The watchdog is one goroutine per process that notices a coroutine running
// longer than its scheduler's stuck timeout without handing off. It reads
// the scheduler's atomic switch counter, so hand-offs pay no timer operation.
// On detection it marks the scheduler broken and wakes Run through the abort
// channel.
var wd struct {
	mu      sync.Mutex
	scheds  map[*Scheduler]struct{}
	running bool
	tick    time.Duration // current check interval of the loop
	wake    chan struct{} // re-plans the loop when a shorter tick is needed
}

// watchdogMinTick bounds the check interval for very small stuck timeouts.
const watchdogMinTick = 5 * time.Millisecond

func watch(s *Scheduler) {
	wd.mu.Lock()
	defer wd.mu.Unlock()
	if wd.scheds == nil {
		wd.scheds = make(map[*Scheduler]struct{})
	}
	wd.scheds[s] = struct{}{}
	if !wd.running {
		wd.running = true
		wd.tick = max(s.stuck/2, watchdogMinTick)
		wd.wake = make(chan struct{}, 1)
		go watchdogLoop(wd.wake)
		return
	}
	if s.stuck/2 < wd.tick {
		select {
		case wd.wake <- struct{}{}:
		default:
		}
	}
}

func unwatch(s *Scheduler) {
	wd.mu.Lock()
	delete(wd.scheds, s)
	wd.mu.Unlock()
}

// watchdogLoop exits when no scheduler is registered; watch restarts it.
func watchdogLoop(wake <-chan struct{}) {
	t := time.NewTimer(watchdogMinTick)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-wake:
		}
		tick := checkSchedulers(time.Now())
		if tick == 0 {
			return
		}
		t.Reset(tick)
	}
}

// checkSchedulers checks every registered scheduler and returns the next
// check interval (half the smallest stuck timeout), or 0 after stopping the
// loop because none is registered.
func checkSchedulers(now time.Time) time.Duration {
	wd.mu.Lock()
	defer wd.mu.Unlock()
	if len(wd.scheds) == 0 {
		wd.running = false
		return 0
	}
	tick := time.Duration(1<<63 - 1)
	for s := range wd.scheds {
		tick = min(tick, s.stuck/2)
		if !s.inRun.Load() || s.broken() {
			s.wdSince = time.Time{}
			continue
		}
		n := s.switches.Load()
		if s.wdSince.IsZero() || n != s.wdSeen {
			s.wdSeen, s.wdSince = n, now
			continue
		}
		if now.Sub(s.wdSince) >= s.stuck && s.ctl.CompareAndSwap(ctlRunning, ctlBroken) {
			s.abort <- struct{}{} // buffered, sent at most once
		}
	}
	wd.tick = max(tick, watchdogMinTick)
	return wd.tick
}
