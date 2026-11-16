package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/imdat99/go-flows/internal/store"
)

// ErrLeaseLost is the cause of a task context canceled because its lease
// could not be renewed.
var ErrLeaseLost = store.ErrLeaseLost

// ErrActivityCanceled is the cause of an activity context canceled because
// the workflow canceled the activity (GW-13). The activity's outcome is
// still recorded.
var ErrActivityCanceled = errors.New("activity canceled by the workflow")

// heartbeat renews the leases of every task this worker holds with one
// batched statement per tick (ported from saga's lease.go). A lease the store
// reports lost cancels its task's context at once. A lease the store reports
// busy (its row locked elsewhere) is retried next tick; it is given up only
// when it has not been renewed for ttl - interval, since past that the
// worker can no longer be sure it still holds it (saga review item 6: a
// transient error must not fail the task, GW-6).
type heartbeat struct {
	st       store.Store
	owner    string
	ttl      time.Duration
	interval time.Duration
	log      *slog.Logger

	mu   sync.Mutex
	held map[heldKey]held // by value: tracking a lease allocates no entry
}

type heldKey struct {
	activity bool
	id       int64
}

type held struct {
	lease   store.Lease
	cancel  context.CancelCauseFunc
	renewed time.Time // local monotonic time of the last successful renewal or claim
}

func newHeartbeat(st store.Store, owner string, ttl time.Duration, log *slog.Logger) *heartbeat {
	return &heartbeat{
		st: st, owner: owner, ttl: ttl, interval: ttl / 3, log: log,
		held: make(map[heldKey]held),
	}
}

// track registers a lease taken at claimed (local time just before the
// claim) and returns a context canceled with ErrLeaseLost when the lease is
// lost, and the tracking to untrack it with (which also cancels the
// context).
func (h *heartbeat) track(ctx context.Context, activity bool, l store.Lease, claimed time.Time) (context.Context, tracking) {
	tctx, cancel := context.WithCancelCause(ctx)
	k := heldKey{activity: activity, id: l.ID}
	h.mu.Lock()
	h.held[k] = held{lease: l, cancel: cancel, renewed: claimed}
	h.mu.Unlock()
	return tctx, tracking{h: h, key: k, lease: l, cancel: cancel}
}

// tracking is a lease tracked by the heartbeat (a value: no closure to
// allocate per lease). The zero value untracks nothing.
type tracking struct {
	h      *heartbeat
	key    heldKey
	lease  store.Lease
	cancel context.CancelCauseFunc
}

// untrack stops tracking the lease and cancels its context; it can be
// called more than once.
func (t tracking) untrack() {
	if t.h == nil {
		return
	}
	t.h.mu.Lock()
	if cur, ok := t.h.held[t.key]; ok && cur.lease == t.lease {
		delete(t.h.held, t.key)
	}
	t.h.mu.Unlock()
	t.cancel(context.Canceled)
}

// run renews every interval until ctx is done.
func (h *heartbeat) run(ctx context.Context) {
	t := time.NewTimer(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		h.tick(ctx)
		t.Reset(h.interval)
	}
}

func (h *heartbeat) tick(ctx context.Context) {
	h.mu.Lock()
	var wf, act []store.Lease
	for k, e := range h.held {
		if k.activity {
			act = append(act, e.lease)
		} else {
			wf = append(wf, e.lease)
		}
	}
	h.mu.Unlock()
	if len(wf) == 0 && len(act) == 0 {
		return
	}
	sent := time.Now()
	r, err := h.st.RenewLeases(ctx, h.owner, wf, act, h.ttl)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			h.log.LogAttrs(ctx, slog.LevelWarn, "flows: lease renewal failed", slog.Any("error", err))
		}
		h.expire(time.Now())
		return
	}
	sentLease, lost, busy := indexReport(wf, act, r)
	h.mu.Lock()
	for _, id := range r.CanceledActivities {
		k := heldKey{true, id}
		if e, ok := h.held[k]; ok && e.lease == sentLease[k] {
			e.cancel(ErrActivityCanceled)
		}
	}
	for k, e := range h.held {
		if sl, ok := sentLease[k]; !ok || e.lease != sl {
			continue // tracked after the snapshot: not part of this report
		}
		switch {
		case lost[k]:
			e.cancel(ErrLeaseLost)
			delete(h.held, k)
		case busy[k]:
		default:
			if !e.renewed.After(sent) {
				e.renewed = sent
				h.held[k] = e // updating the entry of the key ranged over is allowed
			}
		}
	}
	h.mu.Unlock()
	h.expire(time.Now())
}

// expire gives up leases not renewed for ttl - interval: past that point
// another owner may hold them.
func (h *heartbeat) expire(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k, e := range h.held {
		if now.Sub(e.renewed) >= h.ttl-h.interval {
			e.cancel(ErrLeaseLost)
			delete(h.held, k)
		}
	}
}

// renewed records an extension done by the task itself (a resume).
func (h *heartbeat) renewedAt(activity bool, id int64, at time.Time) {
	h.mu.Lock()
	k := heldKey{activity, id}
	if e, ok := h.held[k]; ok && e.renewed.Before(at) {
		e.renewed = at
		h.held[k] = e
	}
	h.mu.Unlock()
}

// cancel cancels a held task's context with cause, keeping its lease
// tracked (its outcome is still recorded).
func (h *heartbeat) cancel(activity bool, id int64, cause error) {
	h.mu.Lock()
	e, ok := h.held[heldKey{activity, id}]
	h.mu.Unlock()
	if ok {
		e.cancel(cause)
	}
}

// indexReport keys a renewal report: the leases sent (reports are about
// them: an entry whose lease changed since, the same execution claimed
// again by this worker, is left alone), and the lost and busy ones.
func indexReport(wf, act []store.Lease, r store.Renewal) (sent map[heldKey]store.Lease, lost, busy map[heldKey]bool) {
	sent = make(map[heldKey]store.Lease, len(wf)+len(act))
	for _, l := range wf {
		sent[heldKey{false, l.ID}] = l
	}
	for _, l := range act {
		sent[heldKey{true, l.ID}] = l
	}
	lost = make(map[heldKey]bool, len(r.LostWorkflows)+len(r.LostActivities))
	busy = make(map[heldKey]bool, len(r.BusyWorkflows)+len(r.BusyActivities))
	for _, id := range r.LostWorkflows {
		lost[heldKey{false, id}] = true
	}
	for _, id := range r.LostActivities {
		lost[heldKey{true, id}] = true
	}
	for _, id := range r.BusyWorkflows {
		busy[heldKey{false, id}] = true
	}
	for _, id := range r.BusyActivities {
		busy[heldKey{true, id}] = true
	}
	return sent, lost, busy
}
