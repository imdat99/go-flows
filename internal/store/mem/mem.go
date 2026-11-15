// Package mem is an in-memory store.Store for unit tests and the workflow
// tester. It honours the same contract as the Postgres store (the storetest
// conformance suite runs against both). Its clock is injectable, so tests can
// move time forward without sleeping.
package mem

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

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

	watchers map[int]func(store.WorkKind, string)
	nextW    int
}

type execution struct {
	store.Execution
	lease       store.Lease
	lockedUntil time.Time
	stickyOwner string
	stickyUntil time.Time
	notBefore   time.Time // released with a delay
	history     []history.Event
	pending     []pending
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
	}
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
		Input: clone(ne.Input), CreatedAt: now,
	}
	s.executions[e.ID] = e
	s.latest[ne.InstanceID] = e.ID
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
	for _, e := range s.executions {
		if !claimable(e.Status) || !contains(queues, e.Queue) {
			continue
		}
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
		e.lockedUntil = now.Add(ttl)
		out = append(out, e.task(now))
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
func (s *Store) CompleteWorkflowTask(_ context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	e, err := s.fenced(l)
	if err != nil {
		return nil, err
	}
	if err := s.validate(e, c); err != nil {
		return nil, err
	}
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

	if c.Status != "" && c.Status != e.Status {
		e.Status = c.Status
	}
	if c.Status == store.StatusNeedsAttention {
		e.Err, e.ErrKinds = c.Err, c.ErrKinds // the reason, for an operator
	}
	if !e.Status.Active() {
		s.end(e, c, now)
	}
	if c.KeepFor > 0 && claimable(e.Status) {
		e.lockedUntil = now.Add(c.KeepFor)
		return leases, nil
	}
	e.lockedUntil = time.Time{}
	e.lease.Owner = ""
	e.stickyOwner, e.stickyUntil = "", time.Time{}
	if len(e.visiblePending(now)) > 0 {
		s.hint(store.WorkflowWork, e.Queue)
	}
	if c.StickyFor > 0 && e.Status.Active() {
		e.stickyOwner, e.stickyUntil = l.Owner, now.Add(c.StickyFor)
	}
	return leases, nil
}

// cancelRequests marks canceled activities and asks children to cancel.
func (s *Store) cancelRequests(e *execution, c *store.Completion, now time.Time) {
	for _, t := range s.tasks {
		if t.ExecutionID == e.ID && !t.Compensate && slices.Contains(c.CanceledActivities, t.ScheduleID) {
			t.cancelRequested = true
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
func (s *Store) validate(e *execution, c *store.Completion) error {
	last := int64(0)
	if n := len(e.history); n > 0 {
		last = e.history[n-1].Seq
	}
	for _, ev := range c.History {
		if ev.Seq != last+1 {
			return store.ErrInvalidCompletion
		}
		last = ev.Seq
	}
	if c.ContinueAsNew != nil && c.Status != store.StatusContinued {
		return store.ErrInvalidCompletion
	}
	if c.Status != "" && !c.Status.Active() && (len(c.Activities) > 0 || len(c.Timers) > 0 || len(c.Children) > 0) {
		return store.ErrInvalidCompletion // they would be dropped at once
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
		if (p.ev.Type == history.SignalReceived || p.ev.Type == history.ExecutionCancelRequested) &&
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
			Queue: a.Queue, Input: clone(a.Input), Compensate: a.Compensate, visibleAt: now,
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
	for _, ch := range children {
		if _, used := s.latest[ch.InstanceID]; used {
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
	// Activity tasks of an ended execution can never deliver: drop them.
	for id, t := range s.tasks {
		if t.ExecutionID == e.ID {
			delete(s.tasks, id)
		}
	}
	if e.ParentID != 0 && e.Status != store.StatusContinued {
		s.notifyParent(e, now)
	}
	if c.ContinueAsNew != nil {
		next := *c.ContinueAsNew
		next.InstanceID, next.ParentID, next.ParentSched = e.InstanceID, e.ParentID, e.ParentSched
		n := s.create(next, now)
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
	s.addPending(p, store.ChildOutcome(e.Execution), now)
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
	if delay <= 0 && len(e.visiblePending(now)) > 0 {
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
		t.Lease.Owner, t.Lease.Version = owner, t.Lease.Version+1
		t.Attempt++
		t.lockedUntil = now.Add(ttl)
		at := t.ActivityTask
		at.Input = clone(at.Input)
		at.CancelRequested = t.cancelRequested
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
	if r.Event.Type != history.ActivityCompleted && r.Event.Type != history.ActivityFailed {
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
func (s *Store) RetryActivityTask(_ context.Context, l store.Lease, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	t, err := s.fencedTask(l)
	if err != nil {
		return err
	}
	t.Lease.Owner, t.lockedUntil, t.visibleAt = "", time.Time{}, now.Add(delay)
	if delay <= 0 {
		s.hint(store.ActivityWork, t.Queue)
	}
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
	e.Input, e.Output = clone(e.Input), clone(e.Output)
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
	}
	for _, l := range activities {
		t, err := s.fencedTask(l)
		if err != nil || l.Owner != owner {
			r.LostActivities = append(r.LostActivities, l.ID)
			continue
		}
		t.lockedUntil = now.Add(ttl)
		if t.cancelRequested {
			r.CanceledActivities = append(r.CanceledActivities, l.ID)
		}
	}
	return r, nil
}

// PurgeEnded implements store.Store.
func (s *Store) PurgeEnded(_ context.Context, olderThan time.Duration, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.clock.Now().Add(-olderThan)
	var ids []int64
	for id, e := range s.executions {
		if !e.EndedAt.IsZero() && e.EndedAt.Before(cutoff) {
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
func (s *Store) Terminate(_ context.Context, instanceID, reason string) error {
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
	now := s.clock.Now()
	seq := int64(1)
	if n := len(e.history); n > 0 {
		seq = e.history[n-1].Seq + 1
	}
	e.history = append(e.history, history.Event{Seq: seq, Type: history.ExecutionTerminated, Err: reason})
	e.Status = store.StatusTerminated
	// The holder's lease is gone: a new version fences its writes.
	e.lease = store.Lease{ID: e.ID, Version: e.lease.Version + 1}
	e.lockedUntil, e.stickyOwner, e.stickyUntil = time.Time{}, "", time.Time{}
	s.end(e, &store.Completion{Status: store.StatusTerminated, Err: reason}, now)
	return nil
}
