// Package mem is an in-memory store.Store for unit tests and the workflow
// tester. It honours the same contract as the Postgres store (the storetest
// conformance suite runs against both). Its clock is injectable, so tests can
// move time forward without sleeping.
package mem

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Clock is the store's time source.
type Clock interface{ Now() time.Time }

// ManualClock is a clock that only moves when told to. The zero value starts
// at the Unix epoch plus one day.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the clock's time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.now.IsZero() {
		c.now = time.Unix(86400, 0).UTC()
	}
	return c.now
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.Now()
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// Store is the in-memory store. The zero value is not usable: use New.
type Store struct {
	mu    sync.Mutex
	clock Clock

	nextID     int64
	executions map[int64]*execution
	latest     map[string]int64 // instance id -> latest execution id
	tasks      map[int64]*activityTask
	locks      map[string]*lockRow // business locks by key
	waiters    map[string][]waiter // lock waiters by key, first come first
	hook       func(ctx context.Context, e store.EndInfo) error

	watchers map[int]func(store.WorkKind, string)
	nextW    int

	// Indexes of executions (T10.8), so that the lookups made per task
	// (claims, Wake, ending a parent) are proportional to the matching set,
	// not to every execution kept for retention. Kept by create, setStatus
	// and PurgeEnded, the only places that add an execution, change its
	// status or remove it; its queue and parent never change.
	runnable map[string]map[int64]*execution // queue -> claimable executions (running, compensating)
	children map[int64][]*execution          // parent execution id -> its child executions, any status

	// visited counts the executions examined by Wake and cancelChildrenOf,
	// for the tests that check those lookups visit only the matching set.
	visited int
}

// SetHook installs the end hook (see pg.Hook), before the store is used.
// It is called with the store locked, before the end is applied; an error
// rejects the completion. It must not call the store.
func (s *Store) SetHook(h func(ctx context.Context, e store.EndInfo) error) { s.hook = h }

func (s *Store) callHook(ctx context.Context, e *execution, status store.Status, msg, kinds string) (err error) {
	if s.hook == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("end hook: panic: %v", r)
		}
	}()
	if err := s.hook(ctx, endInfo(e, status, msg, kinds)); err != nil {
		return fmt.Errorf("end hook: %w", err)
	}
	return nil
}

// waiter is a queued lock request (store.LockRequest.Wait).
type waiter struct {
	exec    int64
	expires time.Time // its place lapses then: next attempt + TTL
}

type lockRow struct {
	exec  int64 // holder execution id
	sched int32 // its acquiring schedule id
	until time.Time
	ttl   time.Duration
	hold  time.Duration // store.LockRequest.Hold
	// reserved: for the waiter a release woke (exec), until its turn ends
	// (until); not extended, taken over silently (T9.8).
	reserved bool
}

type execution struct {
	store.Execution
	lease       store.Lease
	lockedUntil time.Time
	stickyOwner string
	stickyUntil time.Time
	ownerDue    bool      // the next due time is the sticky owner's (ownerDue)
	notBefore   time.Time // released with a delay
	history     []history.Event
	pending     []pending
	// compRequested: a completed child reopened by its parent's
	// compensation; its end answers the parent with CompensationCompleted.
	compRequested bool
	// forceReady: reopened (ReopenParked), claimable even with nothing
	// pending, until claimed.
	forceReady bool
	// stuckSeq is the history position of the last stall (NoteStuck).
	stuckSeq   int64
	stuckNoted bool
	// claims since the last completion, less releases (WorkflowTask.Claims).
	claims int32
}

type pending struct {
	id        int64
	ev        history.Event
	visibleAt time.Time
	seq       int64 // arrival order
}

type activityTask struct {
	store.ActivityTask
	visibleAt       time.Time
	lockedUntil     time.Time
	created         int64
	cancelRequested bool
	maxAttempts     int32 // store.NewActivityTask.MaxAttempts
}

var (
	_ store.Store   = (*Store)(nil)
	_ store.Watcher = (*Store)(nil)
)

// New returns an empty store using clock (nil means the wall clock).
func New(clock Clock) *Store {
	if clock == nil {
		clock = wallClock{}
	}
	return &Store{
		clock:      clock,
		executions: make(map[int64]*execution),
		latest:     make(map[string]int64),
		tasks:      make(map[int64]*activityTask),
		locks:      make(map[string]*lockRow),
		waiters:    make(map[string][]waiter),
		runnable:   make(map[string]map[int64]*execution),
		children:   make(map[int64][]*execution),
	}
}

// setStatus changes e's status, keeping the runnable index.
func (s *Store) setStatus(e *execution, st store.Status) {
	was, is := claimable(e.Status), claimable(st)
	e.Status = st
	switch {
	case is && !was:
		s.addRunnable(e)
	case was && !is:
		s.removeRunnable(e)
	}
}

// addRunnable indexes e as claimable.
func (s *Store) addRunnable(e *execution) {
	m := s.runnable[e.Queue]
	if m == nil {
		m = make(map[int64]*execution)
		s.runnable[e.Queue] = m
	}
	m[e.ID] = e
}

// removeRunnable drops e from the claimable index. A queue's map goes
// once it is empty: a map keeps its peak capacity, and ranging over it
// costs that capacity, so a burst would make every later lookup of the
// queue as slow as a full scan.
func (s *Store) removeRunnable(e *execution) {
	m := s.runnable[e.Queue]
	delete(m, e.ID)
	if len(m) == 0 {
		delete(s.runnable, e.Queue)
	}
}

// index adds a new execution to the indexes.
func (s *Store) index(e *execution) {
	if claimable(e.Status) {
		s.addRunnable(e)
	}
	if e.ParentID != 0 {
		s.children[e.ParentID] = append(s.children[e.ParentID], e)
	}
}

// unindex removes a purged execution from the indexes. Its own children
// keep their entry until they are purged in turn.
func (s *Store) unindex(e *execution) {
	s.removeRunnable(e)
	if e.ParentID == 0 {
		return
	}
	sib := slices.DeleteFunc(s.children[e.ParentID], func(c *execution) bool { return c == e })
	if len(sib) == 0 {
		delete(s.children, e.ParentID)
	} else {
		s.children[e.ParentID] = sib
	}
}

// runnableIn yields the runnable executions of queues, each queue once
// (a queue repeated in queues is not visited twice).
func (s *Store) runnableIn(queues []string) iter.Seq[*execution] {
	return func(yield func(*execution) bool) {
		for i, q := range queues {
			if slices.Contains(queues[:i], q) {
				continue
			}
			for _, e := range s.runnable[q] {
				if !yield(e) {
					return
				}
			}
		}
	}
}

// earliestPending is the earliest visibility of e's pending events (ok
// false: none).
func (e *execution) earliestPending() (at time.Time, ok bool) {
	for _, p := range e.pending {
		if !ok || p.visibleAt.Before(at) {
			at, ok = p.visibleAt, true
		}
	}
	return at, ok
}

func (s *Store) id() int64 {
	s.nextID++
	return s.nextID
}

// Now returns the store clock.
func (s *Store) Now(context.Context) (time.Time, error) { return s.clock.Now(), nil }

// CreateExecution implements store.Store.
func (s *Store) CreateExecution(_ context.Context, ne store.NewExecution) (store.Execution, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.latest[ne.InstanceID]; ok {
		e := s.executions[id]
		if e.Workflow != ne.Workflow {
			return store.Execution{}, false, store.ErrInstanceConflict
		}
		return cloneExecution(e.Execution), false, nil
	}
	e := s.create(ne, s.clock.Now())
	return cloneExecution(e.Execution), true, nil
}

func (s *Store) create(ne store.NewExecution, now time.Time) *execution {
	e := &execution{
		ID: s.id(), InstanceID: ne.InstanceID, Workflow: ne.Workflow, Version: ne.Version, Queue: ne.Queue,
		Status: store.StatusRunning, ParentID: ne.ParentID, ParentSched: ne.ParentSched,
		Input: clone(ne.Input), CreatedAt: now, Trace: clone(ne.Trace),
	}
	s.executions[e.ID] = e
	s.latest[ne.InstanceID] = e.ID
	s.index(e)
	s.hint(store.WorkflowWork, e.Queue)
	s.addPending(e, history.Event{
		Type: history.ExecutionStarted, Name: ne.Workflow, Version: ne.Version, Queue: ne.Queue,
		Payload: clone(ne.Input),
	}, now)
	return e
}

func (s *Store) addPending(e *execution, ev history.Event, visibleAt time.Time) {
	ev.Seq = 0
	id := s.id()
	e.pending = append(e.pending, pending{id: id, ev: ev, visibleAt: visibleAt, seq: id})
}

// Executions returns the executions kept, earlier ones of a
// continue-as-new chain included, in creation (id) order, without their
// Input and Output and with a copy of their Trace (flowtest walks them).
func (s *Store) Executions() []store.Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.Execution, 0, len(s.executions))
	for _, e := range s.executions {
		x := e.Execution
		x.Input, x.Output, x.Trace = nil, nil, clone(x.Trace)
		out = append(out, x)
	}
	slices.SortFunc(out, func(a, b store.Execution) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// GetExecution implements store.Store.
func (s *Store) GetExecution(_ context.Context, instanceID string) (store.Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.latest[instanceID]
	if !ok {
		return store.Execution{}, store.ErrNotFound
	}
	return cloneExecution(s.executions[id].Execution), nil
}

// visiblePending returns the visible pending events in (visibility time,
// arrival) order.
func (e *execution) visiblePending(now time.Time) []pending {
	var out []pending
	for _, p := range e.pending {
		if !p.visibleAt.After(now) {
			out = append(out, p)
		}
	}
	slices.SortStableFunc(out, func(a, b pending) int {
		return cmp.Or(a.visibleAt.Compare(b.visibleAt), cmp.Compare(a.seq, b.seq))
	})
	return out
}

// task builds the workflow task of a claimed or resumed execution.
func (e *execution) task(now time.Time) store.WorkflowTask {
	t := store.WorkflowTask{Lease: e.lease, Execution: cloneExecution(e.Execution), Now: now}
	t.Execution.Input, t.Execution.Output = nil, nil // tasks carry neither (store.WorkflowTask)
	if n := len(e.history); n > 0 {
		t.LastSeq = e.history[n-1].Seq
	}
	for _, p := range e.visiblePending(now) {
		t.Pending = append(t.Pending, store.PendingEvent{ID: p.id, Event: cloneEvent(p.ev)})
	}
	return t
}

func (e *execution) readyAt(now time.Time) (time.Time, bool) {
	var at time.Time
	found := false
	if e.forceReady {
		at, found = now, true
	}
	for _, p := range e.pending {
		if p.visibleAt.After(now) {
			continue
		}
		if !found || p.visibleAt.Before(at) {
			at, found = p.visibleAt, true
		}
	}
	return at, found
}

func contains(queues []string, q string) bool { return slices.Contains(queues, q) }

// Wake reports, for a test driver of virtual time (flowtest), whether some
// task of the queues is claimable by owner now, and otherwise the earliest
// store time at which one becomes claimable (ok false: none ever will
// without new input). Work on other queues is ignored (nobody runs it).
func (s *Store) Wake(owner string, queues []string) (ready bool, next time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	consider := func(at time.Time) {
		if !at.After(now) {
			ready = true
			return
		}
		if !ok || at.Before(next) {
			next, ok = at, true
		}
	}
	for e := range s.runnableIn(queues) {
		s.visited++
		earliest, found := e.earliestPending()
		if e.forceReady {
			earliest, found = now, true
		}
		if !found {
			continue
		}
		at := latest(earliest, e.lockedUntil, e.notBefore)
		if e.stickyOwner != "" && e.stickyOwner != owner {
			at = latest(at, e.stickyUntil)
		}
		consider(at)
	}
	for _, t := range s.tasks {
		if contains(queues, t.Queue) {
			consider(latest(t.visibleAt, t.lockedUntil))
		}
	}
	return ready, next, ok
}

func latest(ts ...time.Time) time.Time {
	var m time.Time
	for _, t := range ts {
		if t.After(m) {
			m = t
		}
	}
	return m
}

// claimable reports whether executions in status s run workflow tasks.
func claimable(s store.Status) bool { return s == store.StatusRunning || s == store.StatusCompensating }

// ClaimWorkflowTasks implements store.Store.
func (s *Store) ClaimWorkflowTasks(_ context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	type cand struct {
		e  *execution
		at time.Time
	}
	var cands []cand
	for e := range s.runnableIn(queues) {
		if e.lockedUntil.After(now) || e.notBefore.After(now) {
			continue
		}
		if e.stickyOwner != "" && e.stickyOwner != owner && e.stickyUntil.After(now) {
			continue
		}
		if at, ok := e.readyAt(now); ok {
			cands = append(cands, cand{e, at})
		}
	}
	slices.SortFunc(cands, func(a, b cand) int {
		return cmp.Or(a.at.Compare(b.at), cmp.Compare(a.e.ID, b.e.ID))
	})
	var out []store.WorkflowTask
	for _, c := range cands {
		if len(out) == limit {
			break
		}
		e := c.e
		e.lease = store.Lease{ID: e.ID, Owner: owner, Version: e.lease.Version + 1}
		e.lockedUntil, e.forceReady = now.Add(ttl), false
		e.claims++
		s.extendLocks(e.ID, now)
		task := e.task(now)
		task.Claims = e.claims
		out = append(out, task)
	}
	return out, nil
}

// LoadHistory implements store.Store.
func (s *Store) LoadHistory(_ context.Context, executionID, afterSeq int64) ([]history.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.executions[executionID]
	if !ok {
		return nil, store.ErrNotFound
	}
	var out []history.Event
	for _, ev := range e.history {
		if ev.Seq > afterSeq {
			out = append(out, cloneEvent(ev))
		}
	}
	return out, nil
}

// fenced returns the execution if l is still its lease (expired but not
// granted again counts as held).
func (s *Store) fenced(l store.Lease) (*execution, error) {
	e, ok := s.executions[l.ID]
	if !ok || l.Owner == "" || e.lease != l {
		return nil, store.ErrLeaseLost
	}
	return e, nil
}

// CompleteWorkflowTask implements store.Store.
func (s *Store) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Resumed = false
	now := s.clock.Now()
	e, err := s.fenced(l)
	if err != nil {
		return nil, err
	}
	if err := s.validate(e, c); err != nil {
		return nil, err
	}
	if store.Ends(c.Status) {
		if err := s.callHook(ctx, e, c.Status, c.Err, c.ErrKinds); err != nil {
			return nil, err
		}
	}
	e.claims = 0 // completed: the claims start over
	for _, ev := range c.History {
		e.history = append(e.history, cloneEvent(ev))
	}
	e.pending = slices.DeleteFunc(e.pending, func(p pending) bool {
		return slices.Contains(c.Consumed, p.id) ||
			p.ev.Type == history.TimerFired && slices.Contains(c.CanceledTimers, p.ev.ScheduleID)
	})
	for _, t := range c.Timers {
		t.Type = history.TimerFired
		s.addPending(e, cloneEvent(t), t.At)
	}
	leases := s.createActivities(e, l.Owner, c.Activities, now)
	s.createChildren(e, c.Children, now)
	s.cancelRequests(e, c, now)
	s.acquireLocks(e, c.Locks, now)
	s.compensateChildren(e, c.CompensateChildren, now)

	if c.Status != "" && c.Status != e.Status {
		s.setStatus(e, c.Status)
	}
	if c.Status == store.StatusNeedsAttention || c.Status == store.StatusCompensating {
		e.Err, e.ErrKinds = c.Err, c.ErrKinds // the reason, for an operator
	}
	if !e.Status.Active() {
		s.end(e, c, now)
	}
	if c.KeepFor > 0 && claimable(e.Status) {
		e.lockedUntil = now.Add(c.KeepFor)
		if c.Resume != nil {
			*c.Resume, c.Resumed = e.task(now), true
		}
		return leases, nil
	}
	e.lockedUntil = time.Time{}
	e.lease.Owner = ""
	e.stickyOwner, e.stickyUntil = "", time.Time{}
	if len(e.visiblePending(now)) > 0 {
		s.hint(store.WorkflowWork, e.Queue)
	}
	e.ownerDue = false
	if c.StickyFor > 0 && e.Status.Active() {
		e.stickyOwner, e.stickyUntil = l.Owner, now.Add(c.StickyFor)
		e.ownerDue = ownerDue(e, c, now)
	}
	return leases, nil
}

// ownerDue tells whether, as Postgres' due_owner, the next due time of an
// execution a completion leaves sticky is its owner's (T10.3): it falls
// within the stickiness and within DueKnown, how far the owner wakes by
// itself for the execution's due times.
func ownerDue(e *execution, c *store.Completion, now time.Time) bool {
	if c.DueKnown <= 0 {
		return false
	}
	first, ok := e.earliestPending()
	return ok && !first.After(now.Add(min(c.StickyFor, c.DueKnown)))
}

// cancelRequests marks canceled activities and asks children to cancel.
func (s *Store) cancelRequests(e *execution, c *store.Completion, now time.Time) {
	for _, t := range s.tasks {
		if t.ExecutionID == e.ID && !t.Compensate && slices.Contains(c.CanceledActivities, t.ScheduleID) {
			t.cancelRequested = true
			if t.Lease.Owner == "" && t.visibleAt.After(now) {
				t.visibleAt = now // waiting for a retry: claimable at once
			}
		}
	}
	for _, id := range c.ChildCancels {
		if cid, ok := s.latest[id]; ok {
			if ch := s.executions[cid]; ch.Status.Active() && ch.ParentID == e.ID {
				s.addPending(ch, history.Event{Type: history.ExecutionCancelRequested}, now)
				s.hint(store.WorkflowWork, ch.Queue)
			}
		}
	}
}

// validate checks c before anything changes: a completion applies
// atomically.
// storableText refuses the text Postgres refuses (a NUL, invalid UTF-8),
// so that tests on this store meet the failure a deployment would: the
// engine must sanitize what it stores.
func storableText(c *store.Completion) error {
	bad := func(v string) bool { return !utf8.ValidString(v) || strings.IndexByte(v, 0) >= 0 }
	if bad(c.Err) || bad(c.ErrKinds) {
		return errors.New("mem: unstorable text in the completion's error (NUL or invalid UTF-8)")
	}
	for i := range c.History {
		if ev := &c.History[i]; bad(ev.Err) || bad(ev.ErrKinds) || bad(ev.Name) {
			return fmt.Errorf("mem: unstorable text in event %d (NUL or invalid UTF-8)", ev.Seq)
		}
	}
	return nil
}

func (s *Store) validate(e *execution, c *store.Completion) error {
	last := int64(0)
	if n := len(e.history); n > 0 {
		last = e.history[n-1].Seq
	}
	if err := storableText(c); err != nil {
		return err
	}
	if !store.ValidShape(c) || len(c.History) > 0 && c.History[0].Seq != last+1 {
		return store.ErrInvalidCompletion
	}
	if s.duplicateActivity(e, c.Activities) {
		return store.ErrInvalidCompletion
	}
	if c.Status != "" && !c.Status.Active() && c.Status != store.StatusContinued && undelivered(e, c.Consumed) {
		return store.ErrPendingEvents
	}
	return nil
}

// duplicateActivity reports two activity tasks for one (schedule id,
// compensate) of e, among acts and the existing ones.
func (s *Store) duplicateActivity(e *execution, acts []store.NewActivityTask) bool {
	type key struct {
		sched int32
		comp  bool
	}
	seen := map[key]bool{}
	for _, t := range s.tasks {
		if t.ExecutionID == e.ID {
			seen[key{t.ScheduleID, t.Compensate}] = true
		}
	}
	for _, a := range acts {
		k := key{a.ScheduleID, a.Compensate}
		if seen[k] {
			return true
		}
		seen[k] = true
	}
	return false
}

// undelivered reports pending signals or cancel requests not in consumed.
func undelivered(e *execution, consumed []int64) bool {
	for _, p := range e.pending {
		if (p.ev.Type == history.SignalReceived || p.ev.Type == history.ExecutionCancelRequested || p.ev.Type == history.LockLost) &&
			!slices.Contains(consumed, p.id) {
			return true
		}
	}
	return false
}

func (s *Store) createActivities(e *execution, owner string, acts []store.NewActivityTask, now time.Time) []store.Lease {
	var leases []store.Lease
	for _, a := range acts {
		t := &activityTask{
			ExecutionID: e.ID, InstanceID: e.InstanceID, ScheduleID: a.ScheduleID, Name: a.Name,
			Queue: a.Queue, Input: clone(a.Input), Compensate: a.Compensate, Uncertain: a.Uncertain, visibleAt: now,
			Trace:       e.Trace, // never modified: shared, copied out by claims
			maxAttempts: a.MaxAttempts,
		}
		t.Lease.ID = s.id()
		t.created = t.Lease.ID
		if a.ClaimFor > 0 {
			t.Lease.Owner, t.Lease.Version = owner, 1
			t.Attempt = 1
			t.lockedUntil = now.Add(a.ClaimFor)
			leases = append(leases, t.Lease)
		}
		s.tasks[t.Lease.ID] = t
		if a.ClaimFor <= 0 {
			s.hint(store.ActivityWork, t.Queue)
		}
	}
	return leases
}

func (s *Store) createChildren(e *execution, children []store.NewExecution, now time.Time) {
	for i, ch := range children {
		_, used := s.latest[ch.InstanceID]
		children[i].Taken = used
		if used {
			s.addPending(e, history.Event{
				Type: history.ChildFailed, ScheduleID: ch.ParentSched, Ref: ch.InstanceID,
				Err: "child instance id already in use", ErrKinds: store.KindInstanceExists, Permanent: true,
			}, now)
			continue
		}
		ch.ParentID = e.ID
		s.create(ch, now)
	}
}

// end finishes e: output, children and parent notification, continue-as-new.
func (s *Store) end(e *execution, c *store.Completion, now time.Time) {
	e.Output, e.Err, e.ErrKinds, e.EndedAt = clone(c.Output), c.Err, c.ErrKinds, now
	var carried []pending // undelivered signals and cancel requests
	for _, p := range e.visiblePending(now) {
		if p.ev.Type == history.SignalReceived || p.ev.Type == history.ExecutionCancelRequested {
			carried = append(carried, p)
		}
	}
	e.pending = nil
	s.releaseLocks(e.ID)
	// Activity tasks of an ended execution can never deliver: drop them.
	for id, t := range s.tasks {
		if t.ExecutionID == e.ID {
			delete(s.tasks, id)
		}
	}
	if e.ParentID != 0 && e.Status != store.StatusContinued {
		s.notifyParent(e, now)
	}
	if e.Status != store.StatusContinued {
		s.cancelChildrenOf(e.ID, now)
	}
	if c.ContinueAsNew != nil {
		next := *c.ContinueAsNew
		next.InstanceID, next.ParentID, next.ParentSched = e.InstanceID, e.ParentID, e.ParentSched
		n := s.create(next, now)
		for _, ev := range c.Carry {
			s.addPending(n, history.Event{Type: ev.Type, Name: ev.Name, Payload: clone(ev.Payload)}, now)
		}
		for _, p := range carried {
			s.addPending(n, p.ev, now)
		}
	}
}

// notifyParent delivers the child's outcome to its active parent.
func (s *Store) notifyParent(e *execution, now time.Time) {
	p, ok := s.executions[e.ParentID]
	if !ok || !p.Status.Active() {
		return
	}
	ev := store.ChildOutcome(e.Execution)
	if e.compRequested {
		ev = history.Event{Type: history.CompensationCompleted, ScheduleID: e.ParentSched, Ref: e.InstanceID}
	}
	s.addPending(p, ev, now)
	s.hint(store.WorkflowWork, p.Queue)
}

// ExtendWorkflowTask implements store.Store.
func (s *Store) ExtendWorkflowTask(_ context.Context, l store.Lease, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	e, err := s.fenced(l)
	if err != nil {
		return err
	}
	e.lockedUntil = now.Add(ttl)
	return nil
}

// ReleaseWorkflowTask implements store.Store.
func (s *Store) ReleaseWorkflowTask(_ context.Context, l store.Lease, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	e, err := s.fenced(l)
	if err != nil {
		return err
	}
	e.lockedUntil, e.lease.Owner = time.Time{}, ""
	e.stickyOwner, e.stickyUntil = "", time.Time{}
	e.notBefore = now.Add(delay)
	if e.claims > 0 {
		e.claims-- // undoes only its own claim: claims lost before it still count
	}
	if len(e.pending) > 0 { // now or after the delay: announced (T9.1)
		s.hint(store.WorkflowWork, e.Queue)
	}
	return nil
}

// ResumeWorkflowTask implements store.Store.
func (s *Store) ResumeWorkflowTask(_ context.Context, l store.Lease, ttl time.Duration) (store.WorkflowTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume(l, ttl, s.clock.Now())
}

func (s *Store) resume(l store.Lease, ttl time.Duration, now time.Time) (store.WorkflowTask, error) {
	e, err := s.fenced(l)
	if err != nil || !claimable(e.Status) {
		return store.WorkflowTask{}, store.ErrLeaseLost
	}
	e.lockedUntil = now.Add(ttl)
	return e.task(now), nil
}

// ClaimActivityTasks implements store.Store.
func (s *Store) ClaimActivityTasks(_ context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var cands []*activityTask
	for _, t := range s.tasks {
		if !contains(queues, t.Queue) || t.visibleAt.After(now) || t.lockedUntil.After(now) {
			continue
		}
		cands = append(cands, t)
	}
	slices.SortFunc(cands, func(a, b *activityTask) int {
		return cmp.Or(a.visibleAt.Compare(b.visibleAt), cmp.Compare(a.created, b.created))
	})
	var out []store.ActivityTask
	for _, t := range cands {
		if len(out) == limit {
			break
		}
		lost := t.Lease.Owner != "" // its lease expired without a result
		if lost && !t.Compensate {
			t.Uncertain = true // the attempt may have run
		}
		t.Lease.Owner, t.Lease.Version = owner, t.Lease.Version+1
		t.Attempt++
		t.lockedUntil = now.Add(ttl)
		s.extendLocks(t.ExecutionID, now)
		at := t.ActivityTask
		at.Input, at.CompData, at.Trace = clone(at.Input), clone(at.CompData), clone(at.Trace)
		at.CancelRequested = t.cancelRequested
		at.Exhausted = lost && t.maxAttempts > 0 && t.Attempt > t.maxAttempts
		out = append(out, at)
	}
	return out, nil
}

func (s *Store) fencedTask(l store.Lease) (*activityTask, error) {
	t, ok := s.tasks[l.ID]
	if !ok || l.Owner == "" || t.Lease != l {
		return nil, store.ErrLeaseLost
	}
	return t, nil
}

// ExtendActivityTask implements store.Store.
func (s *Store) ExtendActivityTask(_ context.Context, l store.Lease, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	t, err := s.fencedTask(l)
	if err != nil {
		return err
	}
	t.lockedUntil = now.Add(ttl)
	return nil
}

// CompleteActivityTask implements store.Store.
func (s *Store) CompleteActivityTask(_ context.Context, l store.Lease, r store.ActivityResult) (*store.WorkflowTask, error) {
	if !store.ValidResult(r.Event.Type) {
		return nil, store.ErrInvalidCompletion
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	t, err := s.fencedTask(l)
	if err != nil {
		return nil, err
	}
	delete(s.tasks, l.ID)
	if e, ok := s.executions[t.ExecutionID]; ok && e.Status.Active() {
		ev := cloneEvent(r.Event)
		ev.ScheduleID, ev.Attempt = t.ScheduleID, t.Attempt
		s.addPending(e, ev, now)
		if !e.lockedUntil.After(now) {
			s.hint(store.WorkflowWork, e.Queue)
		}
	}
	if r.Resume == nil || r.Resume.ID != t.ExecutionID {
		return nil, nil
	}
	task, err := s.resume(*r.Resume, r.ResumeFor, now)
	if err != nil {
		return nil, nil // documented: a lost workflow lease returns no task
	}
	return &task, nil
}

// RetryActivityTask implements store.Store.
func (s *Store) RetryActivityTask(_ context.Context, l store.Lease, delay time.Duration, uncertain bool, compData []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	t, err := s.fencedTask(l)
	if err != nil {
		return err
	}
	t.Lease.Owner, t.lockedUntil, t.visibleAt = "", time.Time{}, now.Add(delay)
	t.Uncertain = t.Uncertain || uncertain && !t.Compensate
	if compData != nil && !t.Compensate {
		t.CompData = clone(compData)
	}
	s.hint(store.ActivityWork, t.Queue) // a delayed retry is announced too (T9.1)
	return nil
}

// Signal implements store.Store.
func (s *Store) Signal(_ context.Context, instanceID, name string, payload []byte) error {
	return s.deliver(instanceID, history.Event{Type: history.SignalReceived, Name: name, Payload: clone(payload)})
}

// RequestCancel implements store.Store.
func (s *Store) RequestCancel(_ context.Context, instanceID string) error {
	return s.deliver(instanceID, history.Event{Type: history.ExecutionCancelRequested})
}

// acquireLocks answers the lock requests of e.
func (s *Store) acquireLocks(e *execution, reqs []store.LockRequest, now time.Time) {
	for _, r := range reqs {
		k, ok := s.locks[r.Key]
		switch {
		case ok && k.exec != e.ID && k.reserved && k.until.After(now) && s.waiting(k.exec, r.Key):
			// The turn of the waiter a release woke (T9.8).
			s.addPending(e, history.Event{Type: history.LockHeld, ScheduleID: r.ScheduleID, Name: r.Key, Err: s.executions[k.exec].InstanceID}, now)
			s.queue(e.ID, r, now)
			continue
		case ok && k.exec != e.ID && !k.reserved && (k.until.After(now) || s.running(k.exec, now) ||
			k.until.Add(k.hold).After(now) && (s.hasActivity(k.exec) || s.hasWaitingEvents(k.exec, now))):
			holder := s.executions[k.exec].InstanceID
			s.addPending(e, history.Event{Type: history.LockHeld, ScheduleID: r.ScheduleID, Name: r.Key, Err: holder}, now)
			s.queue(e.ID, r, now)
			continue
		case ok && k.exec != e.ID && !k.reserved: // expired: taken over; its holder learns it
			if prev := s.executions[k.exec]; prev != nil && prev.Status.Active() {
				s.addPending(prev, history.Event{Type: history.LockLost, ScheduleID: k.sched, Name: r.Key}, now)
				s.hint(store.WorkflowWork, prev.Queue)
			}
		}
		s.locks[r.Key] = &lockRow{exec: e.ID, sched: r.ScheduleID, until: now.Add(r.TTL), ttl: r.TTL, hold: r.Hold}
		s.unqueue(r.Key, e.ID)
		s.addPending(e, history.Event{Type: history.LockAcquired, ScheduleID: r.ScheduleID, Name: r.Key}, now)
	}
}

// running reports whether execution id holds a live lease or has an
// activity running: its expired locks are still its own.
func (s *Store) running(id int64, now time.Time) bool {
	if e := s.executions[id]; e != nil && e.lease.Owner != "" && e.lockedUntil.After(now) {
		return true
	}
	for _, t := range s.tasks {
		if t.ExecutionID == id && t.Lease.Owner != "" && t.lockedUntil.After(now) {
			return true
		}
	}
	return false
}

// hasActivity reports whether execution id has an activity task, running
// or waiting (in its queue, for a retry, or for a new lease).
func (s *Store) hasActivity(id int64) bool {
	for _, t := range s.tasks {
		if t.ExecutionID == id {
			return true
		}
	}
	return false
}

// hasWaitingEvents reports whether execution id has visible pending events
// (T10.5): work waiting for its workflow task, such as an activity's
// result. A future timer is not, nor the events of a parked execution.
func (s *Store) hasWaitingEvents(id int64, now time.Time) bool {
	e := s.executions[id]
	return e != nil && claimable(e.Status) &&
		slices.ContainsFunc(e.pending, func(p pending) bool { return !p.visibleAt.After(now) })
}

// extendLocks extends the locks held by execution id by their TTL.
func (s *Store) extendLocks(id int64, now time.Time) {
	for _, k := range s.locks {
		if k.exec == id && !k.reserved {
			k.until = now.Add(k.ttl)
		}
	}
}

// releaseLocks releases the locks held by execution id, waking the waiter
// whose turn each is, and drops its own waits.
func (s *Store) releaseLocks(id int64) {
	now := s.clock.Now()
	for key := range s.waiters {
		s.unqueue(key, id)
	}
	for key, k := range s.locks {
		if k.exec == id {
			delete(s.locks, key)
			if k.reserved {
				continue // a woken waiter leaving wakes nobody
			}
			if w := s.firstLive(key, id, now); w != nil {
				s.wake(s.executions[w.exec], key, now)
				turn := now.Add(k.ttl) // its turn: one TTL to come
				if w.expires.Before(turn) {
					w.expires = turn
				}
				s.locks[key] = &lockRow{exec: w.exec, until: turn, ttl: k.ttl, reserved: true}
			}
		}
	}
}

// firstLive returns key's first waiter other than execution id still
// waiting: its place not lapsed, its execution active, and sleeping on its
// lock-wait timer (a wait canceled or given up has none: skipped). Lapsed
// and inactive waiters are dropped.
func (s *Store) firstLive(key string, id int64, now time.Time) *waiter {
	ws := s.waiters[key]
	keep := ws[:0]
	var first *waiter
	for i := range ws {
		e := s.executions[ws[i].exec]
		if e == nil || !e.Status.Active() || !ws[i].expires.After(now) {
			continue // dropped
		}
		keep = append(keep, ws[i])
		if first == nil && ws[i].exec != id && claimable(e.Status) && e.hasTimer(store.LockWaitTimer(key)) {
			first = &keep[len(keep)-1]
		}
	}
	if len(keep) == 0 {
		delete(s.waiters, key)
	} else {
		s.waiters[key] = keep
	}
	return first
}

// waiting reports whether execution id is active and sleeps on its
// lock-wait timer for key (T9.8).
func (s *Store) waiting(id int64, key string) bool {
	e := s.executions[id]
	return e != nil && claimable(e.Status) && e.hasTimer(store.LockWaitTimer(key)) // not parked
}

// queue queues (or refreshes, keeping its place) execution id's wait for
// r.Key; a request without Wait leaves the queue.
func (s *Store) queue(id int64, r store.LockRequest, now time.Time) {
	if !r.Wait {
		s.unqueue(r.Key, id)
		return
	}
	expires := now.Add(r.WaitRetry + r.TTL)
	ws := s.waiters[r.Key]
	for i := range ws {
		if ws[i].exec == id {
			if ws[i].expires.After(now) {
				ws[i].expires = expires
				return
			}
			ws = append(ws[:i], ws[i+1:]...) // lapsed: to the back
			break
		}
	}
	s.waiters[r.Key] = append(ws, waiter{exec: id, expires: expires})
}

func (s *Store) unqueue(key string, id int64) {
	ws := s.waiters[key]
	for i := range ws {
		if ws[i].exec == id {
			s.waiters[key] = append(ws[:i], ws[i+1:]...)
			return
		}
	}
}

// hasTimer reports whether a timer called name is pending for e (fired
// or not).
func (e *execution) hasTimer(name string) bool {
	for i := range e.pending {
		if p := &e.pending[i]; p.ev.Type == history.TimerFired && p.ev.Name == name {
			return true
		}
	}
	return false
}

// wake fires e's lock-wait timers for key now (it asks again at once).
func (s *Store) wake(e *execution, key string, now time.Time) {
	name := store.LockWaitTimer(key)
	for i := range e.pending {
		p := &e.pending[i]
		if p.ev.Type == history.TimerFired && p.ev.Name == name && p.visibleAt.After(now) {
			p.visibleAt = now
			s.hint(store.WorkflowWork, e.Queue)
		}
	}
}

// Unstick implements store.Store.
func (s *Store) Unstick(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	for _, e := range s.executions {
		if e.stickyOwner == owner && e.stickyUntil.After(now) {
			e.stickyOwner, e.stickyUntil = "", time.Time{}
			s.hint(store.WorkflowWork, e.Queue) // as Postgres: the others learn its due time (T9.1)
		}
	}
	return nil
}

// compensateChildren reopens the completed children of e at scheds for
// compensation, or answers at once for the others.
func (s *Store) compensateChildren(e *execution, scheds []int32, now time.Time) {
	for _, sched := range scheds {
		var child *execution
		for _, ch := range s.children[e.ID] {
			if ch.ParentSched == sched && ch.Status == store.StatusCompleted {
				child = ch
			}
		}
		if child == nil {
			s.addPending(e, history.Event{Type: history.CompensationCompleted, ScheduleID: sched}, now)
			continue
		}
		s.setStatus(child, store.StatusCompensating)
		child.EndedAt, child.compRequested = time.Time{}, true
		s.addPending(child, history.Event{Type: history.ExecutionCompensationRequested}, now)
		s.hint(store.WorkflowWork, child.Queue)
	}
}

// cancelChildrenOf asks the active children of execution id to cancel.
func (s *Store) cancelChildrenOf(id int64, now time.Time) {
	for _, ch := range s.children[id] {
		s.visited++
		if ch.Status.Active() {
			s.addPending(ch, history.Event{Type: history.ExecutionCancelRequested}, now)
			s.hint(store.WorkflowWork, ch.Queue)
		}
	}
}

// ReopenCompensation implements store.Store.
func (s *Store) ReopenCompensation(_ context.Context, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.latest[instanceID]
	if !ok {
		return store.ErrNotFound
	}
	e := s.executions[id]
	if e.Status != store.StatusNeedsAttention || !slices.Contains(strings.Split(e.ErrKinds, ","), store.KindCompensationFailed) {
		return store.ErrNotActive
	}
	s.setStatus(e, store.StatusCompensating)
	s.addPending(e, history.Event{Type: history.CompensationRetryRequested}, s.clock.Now())
	s.hint(store.WorkflowWork, e.Queue)
	return nil
}

// ReopenParked implements store.Store.
func (s *Store) ReopenParked(_ context.Context, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.latest[instanceID]
	if !ok {
		return store.ErrNotFound
	}
	e := s.executions[id]
	kinds := strings.Split(e.ErrKinds, ",")
	if e.Status != store.StatusNeedsAttention ||
		!slices.Contains(kinds, store.KindNonDeterministic) && !slices.Contains(kinds, store.KindStuck) &&
			!slices.Contains(kinds, store.KindCrashLoop) {
		return store.ErrNotActive
	}
	s.setStatus(e, store.StatusRunning)
	e.Err, e.ErrKinds = "", ""
	e.stuckNoted = false // the stall count starts over (T9.3)
	e.claims = 0         // and the claim count (T11.3)
	e.forceReady = true
	s.hint(store.WorkflowWork, e.Queue)
	return nil
}

func (s *Store) deliver(instanceID string, ev history.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.latest[instanceID]
	if !ok {
		return store.ErrNotFound
	}
	e := s.executions[id]
	if !e.Status.Active() {
		return store.ErrNotActive
	}
	s.addPending(e, ev, s.clock.Now())
	s.hint(store.WorkflowWork, e.Queue)
	return nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

func cloneEvent(ev history.Event) history.Event {
	ev.Payload, ev.CompData = clone(ev.Payload), clone(ev.CompData)
	return ev
}

func cloneExecution(e store.Execution) store.Execution {
	e.Input, e.Output, e.Trace = clone(e.Input), clone(e.Output), clone(e.Trace)
	return e
}

// RenewLeases implements store.Store. Nothing is ever busy in memory.
func (s *Store) RenewLeases(_ context.Context, owner string, workflows, activities []store.Lease, ttl time.Duration) (store.Renewal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var r store.Renewal
	for _, l := range workflows {
		e, err := s.fenced(l)
		if err != nil || l.Owner != owner || !claimable(e.Status) {
			r.LostWorkflows = append(r.LostWorkflows, l.ID)
			continue
		}
		e.lockedUntil = now.Add(ttl)
		s.extendLocks(e.ID, now)
	}
	for _, l := range activities {
		t, err := s.fencedTask(l)
		if err != nil || l.Owner != owner {
			r.LostActivities = append(r.LostActivities, l.ID)
			continue
		}
		t.lockedUntil = now.Add(ttl)
		s.extendLocks(t.ExecutionID, now) // locks live while their execution's activities run
		if t.cancelRequested {
			r.CanceledActivities = append(r.CanceledActivities, l.ID)
		}
	}
	return r, nil
}

// activeAncestor reports whether an ancestor of e is active: compensation
// reaches e through it (a reopened child compensates its own children).
func (s *Store) activeAncestor(e *execution) bool {
	for p, ok := s.executions[e.ParentID]; ok; p, ok = s.executions[p.ParentID] {
		if p.Status.Active() {
			return true
		}
	}
	return false
}

// PurgeEnded implements store.Store.
func (s *Store) PurgeEnded(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.clock.Now().Add(-olderThan)
	var ids []int64
	for id, e := range s.executions {
		if !e.EndedAt.IsZero() && e.EndedAt.Before(cutoff) && !s.activeAncestor(e) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	for _, id := range ids {
		e := s.executions[id]
		delete(s.executions, id)
		s.unindex(e)
		if s.latest[e.InstanceID] == id {
			delete(s.latest, e.InstanceID)
		}
		for tid, t := range s.tasks {
			if t.ExecutionID == id {
				delete(s.tasks, tid)
			}
		}
	}
	return len(ids), nil
}

// Watch implements store.Watcher. wake is called with the store's lock
// held: it must not call the store (signal a channel instead).
func (s *Store) Watch(ctx context.Context, wake func(store.WorkKind, string)) error {
	s.mu.Lock()
	if s.watchers == nil {
		s.watchers = make(map[int]func(store.WorkKind, string))
	}
	id := s.nextW
	s.nextW++
	s.watchers[id] = wake
	s.mu.Unlock()
	wake(store.WorkflowWork, "") // listening: every queue (store.Watcher)
	wake(store.ActivityWork, "")
	<-ctx.Done()
	s.mu.Lock()
	delete(s.watchers, id)
	s.mu.Unlock()
	return ctx.Err()
}

// hint notifies watchers; s.mu is held. Generous: a spurious hint only
// costs a poll.
func (s *Store) hint(kind store.WorkKind, queue string) {
	for _, w := range s.watchers {
		w(kind, queue)
	}
}

// Terminate implements store.Store.
func (s *Store) Terminate(ctx context.Context, instanceID, reason string) (store.EndInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.latest[instanceID]
	if !ok {
		return store.EndInfo{}, store.ErrNotFound
	}
	e := s.executions[id]
	if !e.Status.Active() {
		return store.EndInfo{}, store.ErrNotActive
	}
	if err := s.callHook(ctx, e, store.StatusTerminated, reason, ""); err != nil {
		return store.EndInfo{}, err
	}
	now := s.clock.Now()
	seq := int64(1)
	if n := len(e.history); n > 0 {
		seq = e.history[n-1].Seq + 1
	}
	e.history = append(e.history, history.Event{Seq: seq, Type: history.ExecutionTerminated, Err: reason})
	s.setStatus(e, store.StatusTerminated)
	s.releaseLocks(e.ID)
	// The holder's lease is gone: a new version fences its writes.
	e.lease = store.Lease{ID: e.ID, Version: e.lease.Version + 1}
	e.lockedUntil, e.stickyOwner, e.stickyUntil = time.Time{}, "", time.Time{}
	s.end(e, &store.Completion{Status: store.StatusTerminated, Err: reason}, now)
	return endInfo(e, store.StatusTerminated, reason, ""), nil
}

// endInfo is e's identity ending with status, msg and kinds.
func endInfo(e *execution, status store.Status, msg, kinds string) store.EndInfo {
	return store.EndInfo{
		ExecutionID: e.ID, InstanceID: e.InstanceID, Workflow: e.Workflow, Version: e.Version,
		Status: status, Err: msg, ErrKinds: kinds,
	}
}

// ClaimWorkflowTasksDue implements store.DueClaimer.
func (s *Store) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	tasks, err := s.ClaimWorkflowTasks(ctx, owner, queues, limit, ttl)
	if err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var next time.Time
	earlier := func(t time.Time) {
		if t.After(now) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	for e := range s.runnableIn(queues) {
		earlier(e.notBefore)
		// As Postgres' ready_at: the earliest pending event, if none is
		// visible now (then the execution is ready, not due later), of an
		// execution without a lease (a kept lease is its owner's), unless
		// it is its sticky owner's (due_owner, T10.3).
		if at, ok := e.readyAt(now); e.lockedUntil.IsZero() && (!ok || at.After(now)) {
			if e.stickyOwner == "" || !e.ownerDue {
				if first, ok := e.earliestPending(); ok {
					earlier(first)
				}
			}
		}
	}
	return tasks, until(next, now), nil
}

// ClaimActivityTasksDue implements store.DueClaimer.
func (s *Store) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	tasks, err := s.ClaimActivityTasks(ctx, owner, queues, limit, ttl)
	if err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	var next time.Time
	for _, t := range s.tasks {
		// Unleased tasks waiting for a retry delay (a leased task's
		// expiry is recovery's, as in Postgres).
		if contains(queues, t.Queue) && t.Lease.Owner == "" && t.visibleAt.After(now) && (next.IsZero() || t.visibleAt.Before(next)) {
			next = t.visibleAt
		}
	}
	return tasks, until(next, now), nil
}

// until is the duration from now to next (0: none).
func until(next, now time.Time) time.Duration {
	if next.IsZero() {
		return 0
	}
	return next.Sub(now)
}

// NoteStuck implements store.Store.
func (s *Store) NoteStuck(_ context.Context, l store.Lease, seq int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.fenced(l)
	if err != nil {
		return false, err
	}
	again := e.stuckNoted && e.stuckSeq == seq
	e.stuckSeq, e.stuckNoted = seq, true
	return again, nil
}
