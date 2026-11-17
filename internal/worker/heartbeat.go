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
	// The renewal in flight (renewing), started at renewStart; renewCancel
	// cancels its context.
	renewing    bool
	renewStart  time.Time
	renewCancel context.CancelCauseFunc
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

// errRenewalTimeout is the cause of a renewal canceled because it took
// longer than the heartbeat interval.
var errRenewalTimeout = errors.New("flows: lease renewal timed out")

// run renews every interval and gives up the leases not renewed in time
// until ctx is done. The renewals run in their own goroutine, so a renewal
// that hangs (a database that blackholes: failover, a network drop without
// RST) does not delay giving up leases, and is canceled after an interval,
// in time for the next renewal (T11.1).
func (h *heartbeat) run(ctx context.Context) {
	kick := make(chan struct{}, 1) // a renewal tick waits while a renewal runs
	deadline := time.NewTimer(h.interval)
	deadline.Stop() // armed by each renewal
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.renew(ctx, kick, deadline)
	}()
	renew := time.NewTimer(h.interval)
	defer renew.Stop()
	expire := time.NewTimer(h.interval)
	defer expire.Stop()
	for {
		select {
		case <-ctx.Done():
			<-done // a renewal returns once ctx is done
			deadline.Stop()
			return
		case <-renew.C:
			select {
			case kick <- struct{}{}:
			default:
			}
			renew.Reset(h.interval)
		case <-deadline.C:
			h.mu.Lock()
			// The renewal it was armed for, not one started since.
			if h.renewing && time.Since(h.renewStart) >= h.interval {
				h.renewCancel(errRenewalTimeout)
			}
			h.mu.Unlock()
		case <-expire.C:
			expire.Reset(h.expire(time.Now()))
		}
	}
}

// renew runs a renewal per kick until ctx is done. Each renewal arms
// deadline for one interval; when it fires, run cancels the renewal. The
// renewals' context is made again only after such a cancel (no allocation
// per renewal).
func (h *heartbeat) renew(ctx context.Context, kick <-chan struct{}, deadline *time.Timer) {
	var rctx context.Context
	defer func() {
		h.mu.Lock()
		if h.renewCancel != nil {
			h.renewCancel(context.Canceled)
		}
		h.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		}
		h.mu.Lock()
		if rctx == nil || rctx.Err() != nil {
			rctx, h.renewCancel = context.WithCancelCause(ctx) //nolint:fatcontext // from ctx, not nested: replaces a canceled one
		}
		h.renewing, h.renewStart = true, time.Now()
		deadline.Reset(h.interval)
		h.mu.Unlock()
		h.tick(rctx)
		h.mu.Lock()
		h.renewing = false
		deadline.Stop()
		h.mu.Unlock()
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
	// A renewal cut by its deadline may still reach the database and extend
	// leases already given up here: the work was canceled, so this only
	// delays another engine's claim (by up to a ttl); fencing is unchanged.
	r, err := h.st.RenewLeases(ctx, h.owner, wf, act, h.ttl)
	if err != nil {
		if !errors.Is(err, context.Canceled) || errors.Is(context.Cause(ctx), errRenewalTimeout) {
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
// another owner may hold them. It returns when to check again: when the
// next lease falls due, at most an interval from now, so a lease tracked
// in between is given up an interval late at most (run checks on its own
// timer, whether or not a renewal returns).
func (h *heartbeat) expire(now time.Time) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	keep := h.ttl - h.interval
	next := h.interval
	for k, e := range h.held {
		left := keep - now.Sub(e.renewed)
		if left <= 0 {
			e.cancel(ErrLeaseLost)
			delete(h.held, k)
			continue
		}
		next = min(next, left)
	}
	return next
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
