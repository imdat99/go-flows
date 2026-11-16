package worker

import (
	"sync"
	"time"
)

// dueTimes holds the local times at which work this worker committed
// becomes visible: workflow timers and activity retry delays. The store
// sends no hint for such work (it is not ready at commit), so without
// them a poller backed off to PollMax would find it up to PollMax late.
//
// The times are upper bounds of the store's (computed from durations
// measured before the commit), so a poll at one never comes early. They
// are rounded up to a grain (dueGrain) and deduplicated, so the polls they
// cause are at least a grain apart however many timers fall due, and only
// times within a horizon are kept: the timed polls (at most PollMax apart)
// find later work at most PollMax late, small next to its delay. The set
// thus holds at most horizon/grain times. A time whose work went away (a
// timer the workflow canceled) only costs an empty poll.
type dueTimes struct {
	grain, horizon time.Duration
	epoch          time.Time // monotonic origin of the offsets

	// changed receives a signal when the earliest time changed: the
	// poller recomputes its sleep, without claiming before it is due.
	changed chan struct{}

	mu  sync.Mutex
	h   offsetHeap         // pending offsets from epoch, multiples of grain
	set map[int64]struct{} // members of h
}

func newDueTimes(grain, horizon time.Duration) *dueTimes {
	return &dueTimes{grain: grain, horizon: horizon, epoch: time.Now(), set: map[int64]struct{}{}, changed: make(chan struct{}, 1)}
}

// add records that work falls due at at. It signals changed only when at
// became the earliest time, to shorten a longer sleep.
func (d *dueTimes) add(at time.Time) {
	now := time.Now()
	if at.Sub(now) > d.horizon {
		return
	}
	// Due already (its commit took longer than its delay): a claim that
	// started before now may have missed the work (not yet committed),
	// and would drop an earlier time as covered. Due now instead: only a
	// claim started after the commit covers it.
	if at.Before(now) {
		at = now
	}
	off := int64(at.Sub(d.epoch))
	if r := off % int64(d.grain); r != 0 {
		off += int64(d.grain) - r // round up: never before at
	}
	d.mu.Lock()
	first := false
	if _, dup := d.set[off]; !dup {
		d.set[off] = struct{}{}
		d.h.push(off)
		first = d.h[0] == off
	}
	d.mu.Unlock()
	if first {
		notify(d.changed)
	}
}

// next drops the times a claim started at polled covered and returns the
// earliest remaining one. A zero polled (a failed claim) drops nothing.
func (d *dueTimes) next(polled time.Time) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !polled.IsZero() {
		covered := int64(polled.Sub(d.epoch))
		for len(d.h) > 0 && d.h[0] <= covered {
			delete(d.set, d.h[0])
			d.h.pop()
		}
	}
	if len(d.h) == 0 {
		return time.Time{}, false
	}
	return d.epoch.Add(time.Duration(d.h[0])), true
}

// offsetHeap is a binary min-heap.
type offsetHeap []int64

func (h *offsetHeap) push(v int64) {
	*h = append(*h, v)
	a := *h
	for i := len(a) - 1; i > 0; {
		p := (i - 1) / 2
		if a[i] >= a[p] {
			break
		}
		a[i], a[p] = a[p], a[i]
		i = p
	}
}

func (h *offsetHeap) pop() {
	a := *h
	n := len(a) - 1
	a[0] = a[n]
	a = a[:n]
	for i := 0; ; {
		m, l, r := i, 2*i+1, 2*i+2
		if l < n && a[l] < a[m] {
			m = l
		}
		if r < n && a[r] < a[m] {
			m = r
		}
		if m == i {
			break
		}
		a[i], a[m] = a[m], a[i]
		i = m
	}
	*h = a
}
