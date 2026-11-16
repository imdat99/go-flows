package pg

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Statement names of the fast path (prepared once per connection).
var (
	nFence               = stmtName(fence)
	nLockParent          = stmtName(lockParent)
	nNoUndelivered       = stmtName(noUndelivered)
	nInsertHistory       = stmtName(insertHistory)
	nDeletePending       = stmtName(deletePending)
	nInsertPending       = stmtName(insertPending)
	nInsertActs          = stmtName(insertActivities)
	nInsertChild         = stmtName(insertChild)
	nCancelChildren      = stmtName(cancelChildren)
	nCancelActs          = stmtName(cancelActivities)
	nAcquireLock         = stmtName(acquireLock)
	nCompChild           = stmtName(compensateChild)
	nUpdateRunning       = stmtName(updateRunning)
	nEndExecution        = stmtName(endExecution)
	nContinueAsNew       = stmtName(continueAsNew)
	nCarryPending        = stmtName(carryPending)
	nCarrySignals        = stmtName(carrySignals)
	nDropEnded           = stmtName(dropEnded)
	nDropLocks           = stmtName(dropLocks)
	nWakeWaiters         = stmtName(wakeWaiters)
	nLockWaiters         = stmtName(lockWaiters)
	nCancelChildOf       = stmtName(cancelChildrenOf)
	nNotifyParent        = stmtName(notifyParent)
	nWakeForTask         = stmtName(wakeForTask)
	nFinishTask          = stmtName(finishTask)
	nResumeExtend        = stmtName(resumeExtend)
	nResumeWorkflow      = stmtName(resumeWorkflow)
	nResumePending       = stmtName(resumePending)
	nClaimWorkflow       = stmtName(claimWorkflow)
	nClaimWorkflowQueues = stmtName(claimWorkflowQueues)
	nInsertExecution     = stmtName(insertExecution)
	nClaimActivities     = stmtName(claimActivities)
	nNextWorkflowDue     = stmtName(nextWorkflowDue)
	nNextActivityDue     = stmtName(nextActivityDue)
)

// stmt starts and ends one statement of a fast batch.
func (fb *fastBatch) stmt(sql, name string, params func()) {
	fb.begin(sql, name)
	if fb.a.err == nil {
		params()
	}
	fb.end(name)
}

// events encodes the 17 event-array parameters ($2..$18) of insertHistory
// and insertPending.
func (fb *fastBatch) events(evs []history.Event, visible func(i int) time.Time) {
	n := len(evs)
	fb.Ints(n, func(i int) int64 { return int64(evs[i].Type) })
	fb.Ints(n, func(i int) int64 { return int64(evs[i].ScheduleID) })
	fb.Ints(n, func(i int) int64 { return int64(evs[i].Attempt) })
	fb.Strs(n, func(i int) string { return evs[i].Name })
	fb.Ints(n, func(i int) int64 { return int64(evs[i].Version) })
	fb.Strs(n, func(i int) string { return evs[i].Ref })
	fb.Strs(n, func(i int) string { return evs[i].Queue })
	fb.Ints(n, func(i int) int64 { return int64(evs[i].ArgHash) }) // same 64 bits in a bigint
	fb.Blobs(n, func(i int) []byte { return evs[i].Payload })
	fb.Blobs(n, func(i int) []byte { return evs[i].CompData })
	fb.Strs(n, func(i int) string { return evs[i].Err })
	fb.Strs(n, func(i int) string { return evs[i].ErrKinds })
	fb.Bools(n, func(i int) bool { return evs[i].Uncertain })
	fb.Bools(n, func(i int) bool { return evs[i].Permanent })
	fb.Times(n, func(i int) time.Time { return evs[i].At })
	fb.Ints(n, func(i int) int64 { return evs[i].Seq })
	fb.Times(n, visible)
}

func noTime(int) time.Time { return time.Time{} }

// completeFast is CompleteWorkflowTask on the fast path: the same
// statements as queueCompletion, encoded by hand.
//
// It returns the leases of the eagerly claimed activity tasks, in the order
// of c.Activities (as CompleteWorkflowTask), filled from the rows directly.
func (s *Store) completeFast(ctx context.Context, l store.Lease, c *store.Completion, ending, terminal bool,
	prevSeq, lastSeq int64,
) ([]store.Lease, error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer pc.Release()
	fb := s.getFastBatch(ctx, pc.Conn())
	defer fb.release()

	if terminal {
		fb.stmt(lockParent, nLockParent, func() { fb.Int(l.ID) }) // T9.5: parent row first
	}
	fb.stmt(fence, nFence, func() {
		fb.Int(l.ID)
		fb.Str(l.Owner)
		fb.Int(l.Version)
		fb.Int(prevSeq)
		fb.Bool(len(c.History) > 0)
	})
	if terminal {
		fb.stmt(noUndelivered, nNoUndelivered, func() {
			fb.Int(l.ID)
			fb.Ints(len(c.Consumed), func(i int) int64 { return c.Consumed[i] })
		})
	}
	if len(c.History) > 0 {
		fb.stmt(insertHistory, nInsertHistory, func() {
			fb.Int(l.ID)
			fb.events(c.History, noTime)
		})
	}
	if len(c.Consumed) > 0 || len(c.CanceledTimers) > 0 {
		fb.stmt(deletePending, nDeletePending, func() {
			fb.Int(l.ID)
			fb.Ints(len(c.Consumed), func(i int) int64 { return c.Consumed[i] })
			fb.Ints(len(c.CanceledTimers), func(i int) int64 { return int64(c.CanceledTimers[i]) })
		})
	}
	if len(c.Timers) > 0 {
		fb.stmt(insertPending, nInsertPending, func() {
			fb.Int(l.ID)
			timers := c.Timers
			n := len(timers)
			fb.Ints(n, func(int) int64 { return int64(history.TimerFired) })
			fb.Ints(n, func(i int) int64 { return int64(timers[i].ScheduleID) })
			fb.Ints(n, func(i int) int64 { return int64(timers[i].Attempt) })
			fb.Strs(n, func(i int) string { return timers[i].Name })
			fb.Ints(n, func(i int) int64 { return int64(timers[i].Version) })
			fb.Strs(n, func(i int) string { return timers[i].Ref })
			fb.Strs(n, func(i int) string { return timers[i].Queue })
			fb.Ints(n, func(i int) int64 { return int64(timers[i].ArgHash) }) // same 64 bits
			fb.Blobs(n, func(i int) []byte { return timers[i].Payload })
			fb.Blobs(n, func(i int) []byte { return timers[i].CompData })
			fb.Strs(n, func(i int) string { return timers[i].Err })
			fb.Strs(n, func(i int) string { return timers[i].ErrKinds })
			fb.Bools(n, func(i int) bool { return timers[i].Uncertain })
			fb.Bools(n, func(i int) bool { return timers[i].Permanent })
			fb.Times(n, func(i int) time.Time { return timers[i].At })
			fb.Ints(n, func(int) int64 { return 0 })
			fb.Times(n, func(i int) time.Time { return timers[i].At })
		})
	}
	activityAt := -1
	if len(c.Activities) > 0 {
		activityAt = fb.stmts
		acts := c.Activities
		n := len(acts)
		fb.stmt(insertActivities, nInsertActs, func() {
			fb.Int(l.ID)
			fb.Ints(n, func(i int) int64 { return int64(acts[i].ScheduleID) })
			fb.Bools(n, func(i int) bool { return acts[i].Compensate })
			fb.Strs(n, func(i int) string { return acts[i].Name })
			fb.Strs(n, func(i int) string { return acts[i].Queue })
			fb.Blobs(n, func(i int) []byte { return acts[i].Input })
			fb.Ints(n, func(i int) int64 { return micros(acts[i].ClaimFor) })
			fb.Str(l.Owner)
			fb.Bools(n, func(i int) bool { return acts[i].Uncertain })
		})
	}
	childAt := fb.stmts
	for i := range c.Children {
		ch := &c.Children[i]
		ch.Taken = false // set below from its row (retry may run this twice)
		fb.stmt(insertChild, nInsertChild, func() {
			fb.Str(ch.InstanceID)
			fb.Str(ch.Workflow)
			fb.Int(int64(ch.Version))
			fb.Str(ch.Queue)
			fb.Int(l.ID)
			fb.Int(int64(ch.ParentSched))
			fb.Bytes(ch.Input)
			fb.Bytes(ch.Trace)
		})
	}
	if len(c.ChildCancels) > 0 {
		fb.stmt(cancelChildren, nCancelChildren, func() {
			fb.Int(l.ID)
			fb.Strs(len(c.ChildCancels), func(i int) string { return c.ChildCancels[i] })
		})
	}
	if len(c.CanceledActivities) > 0 {
		fb.stmt(cancelActivities, nCancelActs, func() {
			fb.Int(l.ID)
			fb.Ints(len(c.CanceledActivities), func(i int) int64 { return int64(c.CanceledActivities[i]) })
		})
	}
	// Same order as the pgx path (queueCompletion): the pending events
	// these create are numbered, and delivered, in this order.
	locks := sortedLocks(c.Locks)
	for i := range locks {
		k := &locks[i]
		fb.stmt(acquireLock, nAcquireLock, func() {
			fb.Int(l.ID)
			fb.Str(k.Key)
			fb.Int(int64(k.ScheduleID))
			fb.Int(micros(k.TTL))
			fb.Int(micros(k.Hold))
			fb.Bool(k.Wait)
			fb.Int(micros(k.WaitRetry))
		})
	}
	for _, sched := range c.CompensateChildren {
		fb.stmt(compensateChild, nCompChild, func() {
			fb.Int(l.ID)
			fb.Int(int64(sched))
		})
	}
	s.endFast(fb, l, c, ending, lastSeq)
	c.Resumed = false // set again below on success (retry may run this twice)
	resumeAt, prevID := fb.resumeAfter(l, c, ending)
	leases := eagerLeaseSlots(c, l.Owner)
	err = fb.exec(func(i int, v [][]byte) error {
		if k := i - childAt; k >= 0 && k < len(c.Children) {
			c.Children[k].Taken = true // its only row: the id was in use
			return nil
		}
		if i == activityAt {
			if len(v[3]) == 0 { // owner_id empty: not claimed eagerly
				return nil
			}
			return setEagerLease(c, leases, v)
		}
		if resumeAt >= 0 && i > resumeAt {
			return decodeResumed(i-resumeAt, v, c, l.Owner, prevID)
		}
		return nil
	})
	return leases, err
}

// resumeAfter queues the resume of c's kept lease when c asks for it
// (T9.7: the next task in the same round trip; the answers to c.Locks are
// visible, same transaction, same now()) and returns the index of its
// first statement (-1: none) and the instance id c.Resume held.
func (fb *fastBatch) resumeAfter(l store.Lease, c *store.Completion, ending bool) (at int, prevID string) {
	if !resumes(c, ending) {
		return -1, ""
	}
	at, prevID = fb.stmts, c.Resume.Execution.InstanceID
	*c.Resume = store.WorkflowTask{Pending: c.Resume.Pending[:0]} // keep its capacity
	fb.resume(l, c.KeepFor)
	return at, prevID
}

// decodeResumed decodes row v of the k-th statement of fb.resume into
// c.Resume (1: resumeWorkflow, 2: resumePending).
func decodeResumed(k int, v [][]byte, c *store.Completion, owner, prevID string) error {
	switch k {
	case 1:
		if err := decodeTask(v, c.Resume, owner, prevID); err != nil {
			return err
		}
		c.Resumed = true
	case 2:
		p, err := decodePending(v)
		if err != nil {
			return err
		}
		c.Resume.Pending = append(c.Resume.Pending, p)
	}
	return nil
}

// eagerLeaseSlots returns c's leases of eager activities (ids set from the
// rows), in c.Leases' capacity (the completion's scratch space).
func eagerLeaseSlots(c *store.Completion, owner string) []store.Lease {
	leases := c.Leases[:0]
	for _, a := range c.Activities {
		if a.ClaimFor > 0 {
			leases = append(leases, store.Lease{Owner: owner, Version: 1})
		}
	}
	c.Leases = leases
	return leases
}

// setEagerLease sets the id of the lease of the eagerly claimed activity
// task in row v (id, schedule_id, compensate, owner_id); leases are those of
// c's eager activities, in order.
func setEagerLease(c *store.Completion, leases []store.Lease, v [][]byte) error {
	id, err := decodeInt(v[0])
	if err != nil {
		return err
	}
	sched, err := decodeInt(v[1])
	if err != nil {
		return err
	}
	comp := len(v[2]) == 1 && v[2][0] == 1
	j := 0 // the lease of this task: its rank among the eager activities
	for _, a := range c.Activities {
		if a.ClaimFor <= 0 {
			continue
		}
		if a.ScheduleID == int32(sched) && a.Compensate == comp { // int4 column
			leases[j].ID = id
			return nil
		}
		j++
	}
	return fmt.Errorf("%w: claimed activity task %d is not in the completion", errBadValue, sched)
}

// endFast encodes the closing statements (running, or ended).
func (s *Store) endFast(fb *fastBatch, l store.Lease, c *store.Completion, ending bool, lastSeq int64) {
	if !ending {
		fb.stmt(updateRunning, nUpdateRunning, func() {
			fb.Int(l.ID)
			fb.Int(lastSeq)
			fb.Str(string(c.Status))
			fb.Int(micros(c.KeepFor))
			fb.Int(micros(c.StickyFor))
			fb.Str(l.Owner)
			fb.Str(c.Err)
			fb.Str(c.ErrKinds)
			fb.Int(micros(c.DueKnown))
		})
		return
	}
	fb.stmt(endExecution, nEndExecution, func() {
		fb.Int(l.ID)
		fb.Int(lastSeq)
		fb.Str(string(c.Status))
		fb.Bytes(c.Output)
		fb.Str(c.Err)
		fb.Str(c.ErrKinds)
	})
	if n := c.ContinueAsNew; n != nil {
		fb.stmt(continueAsNew, nContinueAsNew, func() {
			fb.Int(l.ID)
			fb.Str(n.Workflow)
			fb.Int(int64(n.Version))
			fb.Str(n.Queue)
			fb.Bytes(n.Input)
			fb.Bytes(n.Trace)
		})
		if n := len(c.Carry); n > 0 {
			fb.stmt(carrySignals, nCarrySignals, func() {
				fb.Int(l.ID)
				fb.Ints(n, func(i int) int64 { return int64(c.Carry[i].Type) })
				fb.Strs(n, func(i int) string { return c.Carry[i].Name })
				fb.Blobs(n, func(i int) []byte { return c.Carry[i].Payload })
			})
		}
		fb.stmt(carryPending, nCarryPending, func() { fb.Int(l.ID) })
	}
	// Children's rows, then task rows, then lock rows (lockParent).
	if c.Status != store.StatusContinued {
		fb.stmt(cancelChildrenOf, nCancelChildOf, func() { fb.Int(l.ID) })
	}
	fb.stmt(dropEnded, nDropEnded, func() { fb.Int(l.ID) })
	fb.stmt(lockWaiters, nLockWaiters, func() { fb.Int(l.ID) }) // T9.8, before the locks go
	fb.stmt(wakeWaiters, nWakeWaiters, func() { fb.Int(l.ID) })
	fb.stmt(dropLocks, nDropLocks, func() { fb.Int(l.ID) })
	if c.Status != store.StatusContinued {
		ev := store.ChildOutcome(store.Execution{Status: c.Status, Output: c.Output, Err: c.Err, ErrKinds: c.ErrKinds})
		fb.stmt(notifyParent, nNotifyParent, func() {
			fb.Int(l.ID)
			fb.Int(int64(ev.Type))
			fb.Bytes(ev.Payload)
			fb.Str(ev.Err)
			fb.Str(ev.ErrKinds)
		})
	}
}

// completeActivityFast is CompleteActivityTask on the fast path.
func (s *Store) completeActivityFast(ctx context.Context, l store.Lease, r *store.ActivityResult) (*store.WorkflowTask, error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer pc.Release()
	fb := s.getFastBatch(ctx, pc.Conn())
	defer fb.release()
	ev := &r.Event
	fb.stmt(wakeForTask, nWakeForTask, func() { fb.Int(l.ID) })
	fb.stmt(finishTask, nFinishTask, func() {
		fb.Int(l.ID)
		fb.Str(l.Owner)
		fb.Int(l.Version)
		fb.Int(int64(ev.Type))
		fb.Bytes(ev.Payload)
		fb.Bytes(ev.CompData)
		fb.Str(ev.Err)
		fb.Str(ev.ErrKinds)
		fb.Bool(ev.Uncertain)
		fb.Bool(ev.Permanent)
	})
	if r.Resume != nil {
		fb.resume(*r.Resume, r.ResumeFor)
	}
	var execID int64
	task := r.Into
	if task == nil {
		task = new(store.WorkflowTask)
	}
	prevID := task.Execution.InstanceID                   // most likely the same execution's
	*task = store.WorkflowTask{Pending: task.Pending[:0]} // keep its capacity
	found := false
	err = fb.exec(func(i int, v [][]byte) error {
		switch i {
		case 1: // finishTask: the execution id
			var err error
			if execID, err = decodeInt(v[0]); err != nil {
				return err
			}
		case 3: // resumeWorkflow
			if err := decodeTask(v, task, r.Resume.Owner, prevID); err != nil {
				return err
			}
			found = true
		case 4: // resumePending
			p, err := decodePending(v)
			if err != nil {
				return err
			}
			task.Pending = append(task.Pending, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Resume == nil || !found || task.Execution.ID != execID {
		return nil, nil
	}
	return task, nil
}

func (fb *fastBatch) resume(l store.Lease, ttl time.Duration) {
	fb.stmt(resumeExtend, nResumeExtend, func() {
		fb.Int(l.ID)
		fb.Str(l.Owner)
		fb.Int(l.Version)
		fb.Int(micros(ttl))
	})
	fb.stmt(resumeWorkflow, nResumeWorkflow, func() {
		fb.Int(l.ID)
		fb.Str(l.Owner)
		fb.Int(l.Version)
	})
	fb.stmt(resumePending, nResumePending, func() {
		fb.Int(l.ID)
		fb.Str(l.Owner)
		fb.Int(l.Version)
	})
}

// execWidth is the number of executionColumns, taskExecWidth of
// taskColumns (without input and output), taskWidth the columns
// decodeTask reads.
const (
	execWidth     = 15
	taskExecWidth = execWidth - 2
	taskWidth     = taskExecWidth + 3
)

// decodeTask decodes taskColumns + lease_version, last_seq, now().
// prevID, when equal to the decoded instance id, is shared rather than
// copied (a resumed task is most likely of the execution of the last one).
func decodeTask(v [][]byte, t *store.WorkflowTask, owner, prevID string) error {
	if len(v) != taskWidth {
		return fmt.Errorf("%w: %d task columns", errBadValue, len(v))
	}
	if err := decodeExecution(v[:taskExecWidth], &t.Execution, prevID); err != nil {
		return err
	}
	var err error
	if t.Lease.Version, err = decodeInt(v[taskExecWidth]); err != nil {
		return err
	}
	if t.LastSeq, err = decodeInt(v[taskExecWidth+1]); err != nil {
		return err
	}
	if t.Now, err = decodeTime(v[taskExecWidth+2]); err != nil {
		return err
	}
	t.Lease.ID, t.Lease.Owner = t.Execution.ID, owner
	return nil
}

// decodeExecution decodes executionColumns, or taskColumns (prevID: see
// decodeTask).
func decodeExecution(v [][]byte, e *store.Execution, prevID string) error {
	o := 0 // offset of the columns after input and output
	switch len(v) {
	case execWidth:
		e.Input, e.Output = cloneBytes(v[8]), cloneBytes(v[9])
		o = 2
	case taskExecWidth:
		e.Input, e.Output = nil, nil
	default:
		return fmt.Errorf("%w: %d execution columns", errBadValue, len(v))
	}
	var err error
	if e.ID, err = decodeInt(v[0]); err != nil {
		return err
	}
	if e.ParentID, err = decodeInt(v[6]); err != nil {
		return err
	}
	version, err := decodeInt(v[3])
	if err != nil {
		return err
	}
	sched, err := decodeInt(v[7])
	if err != nil {
		return err
	}
	e.Version, e.ParentSched = int32(version), int32(sched) // int4 columns
	if string(v[1]) == prevID {                             // a comparison with string(v[1]) does not copy it
		e.InstanceID = prevID
	} else {
		e.InstanceID = string(v[1])
	}
	e.Workflow, e.Queue = intern(v[2]), intern(v[4])
	e.Status = store.Status(intern(v[5]))
	e.Err, e.ErrKinds = string(v[8+o]), string(v[9+o])
	if e.CreatedAt, err = decodeTime(v[10+o]); err != nil {
		return err
	}
	if e.EndedAt, err = decodeTime(v[11+o]); err != nil {
		return err
	}
	e.Trace = cloneBytes(v[12+o]) // NULL without a tracer: nil, no allocation
	return nil
}

// createFast is the insert of CreateExecution on the fast path; ok is
// false when the instance id is taken (no row).
func (s *Store) createFast(ctx context.Context, ne *store.NewExecution) (e store.Execution, ok bool, err error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return e, false, err
	}
	defer pc.Release()
	fb := s.getFastBatch(ctx, pc.Conn())
	defer fb.release()
	fb.oneShot = true // ctx is the application's (Workflow.Start), possibly never canceled
	fb.stmt(insertExecution, nInsertExecution, func() {
		fb.Str(ne.InstanceID)
		fb.Str(ne.Workflow)
		fb.Int(int64(ne.Version))
		fb.Str(ne.Queue)
		fb.Int(ne.ParentID)
		fb.Int(int64(ne.ParentSched))
		fb.Bytes(ne.Input)
		fb.Bytes(ne.Trace)
	})
	err = fb.exec(func(_ int, v [][]byte) error {
		ok = true
		return decodeExecution(v, &e, ne.InstanceID)
	})
	return e, ok, err
}

// decodePending decodes "p.id, p.execution_id, " + event columns.
func decodePending(v [][]byte) (store.PendingEvent, error) {
	var p store.PendingEvent
	if len(v) != 17 {
		return p, fmt.Errorf("%w: %d pending columns", errBadValue, len(v))
	}
	var nums [8]int64
	for i, col := range [...]int{0, 2, 3, 4, 6, 9} {
		n, err := decodeInt(v[col])
		if err != nil {
			return p, err
		}
		nums[i] = n
	}
	ev := &p.Event
	p.ID = nums[0]
	ev.Type = history.Type(nums[1])                                                        // smallint column
	ev.ScheduleID, ev.Attempt, ev.Version = int32(nums[2]), int32(nums[3]), int32(nums[4]) // int4 columns
	ev.ArgHash = uint64(nums[5])                                                           // same 64 bits
	ev.Name, ev.Ref, ev.Queue = string(v[5]), string(v[7]), string(v[8])
	ev.Payload, ev.CompData = cloneBytes(v[10]), cloneBytes(v[11])
	ev.Err, ev.ErrKinds = string(v[12]), string(v[13])
	ev.Uncertain = len(v[14]) == 1 && v[14][0] == 1
	ev.Permanent = len(v[15]) == 1 && v[15][0] == 1
	at, err := decodeTime(v[16])
	ev.At = at
	return p, err
}

// claimFast is ClaimWorkflowTasks on the fast path: claimStatement, its
// pending event arrays decoded by hand (pgx planned a scan per array).
func (s *Store) claimFast(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration, due bool) ([]store.WorkflowTask, time.Duration, error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer pc.Release()
	fb := s.getFastBatch(ctx, pc.Conn())
	defer fb.release()
	sql, name := claimStatement(queues)
	fb.stmt(sql, name, func() {
		fb.Strs(len(queues), func(i int) string { return queues[i] })
		fb.Str(owner)
		fb.Int(int64(limit))
		fb.Int(micros(ttl))
	})
	if due {
		fb.stmt(nextWorkflowDue, nNextWorkflowDue, func() {
			fb.Strs(len(queues), func(i int) string { return queues[i] })
		})
	}
	var tasks []store.WorkflowTask
	var next time.Duration
	err = fb.exec(func(i int, v [][]byte) error {
		if i == 1 {
			return decodeDue(v, &next)
		}
		if len(v) != taskWidth+claimedArrays {
			return fmt.Errorf("%w: %d claim columns", errBadValue, len(v))
		}
		var t store.WorkflowTask
		if err := decodeTask(v[:taskWidth], &t, owner, ""); err != nil {
			return err
		}
		pending, err := fb.decodePendingArrays(v[taskWidth:])
		if err != nil {
			return err
		}
		t.Pending = pending
		tasks = append(tasks, t)
		return nil
	})
	return tasks, next, err
}

// claimedArrays is the number of pending event arrays of a claimed
// execution (pendingAggs: id, then the 15 event columns).
const claimedArrays = 16

// decodePendingArrays decodes pendingAggs' arrays: element j of each is a
// column of event j, laid out as decodePending's row (id, execution id,
// event columns).
func (fb *fastBatch) decodePendingArrays(arrs [][]byte) ([]store.PendingEvent, error) {
	n := -1
	for k, a := range arrs {
		elems, err := decodeArray(a, fb.elems[k][:0])
		if err != nil {
			return nil, err
		}
		fb.elems[k] = elems
		if n >= 0 && len(elems) != n {
			return nil, fmt.Errorf("%w: pending arrays of %d and %d elements", errBadValue, n, len(elems))
		}
		n = len(elems)
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]store.PendingEvent, n)
	var row [17][]byte
	for j := range n {
		row[0] = fb.elems[0][j] // id; row[1], the execution id, is not read
		for k := 1; k < claimedArrays; k++ {
			row[k+1] = fb.elems[k][j]
		}
		p, err := decodePending(row[:])
		if err != nil {
			return nil, err
		}
		out[j] = p
	}
	for k := range fb.elems {
		clear(fb.elems[k]) // do not keep views of the read buffer
	}
	return out, nil
}

// decodeArray appends to dst the elements (nil for NULL) of a binary
// one-dimensional array: views into b.
func decodeArray(b []byte, dst [][]byte) ([][]byte, error) {
	if len(b) < 12 {
		return dst, fmt.Errorf("%w: array of %d bytes", errBadValue, len(b))
	}
	ndim := int32(binary.BigEndian.Uint32(b)) // two's complement
	b = b[12:]                                // ndim, has-null flag, element type
	switch ndim {
	case 0:
		return dst, nil
	case 1:
	default:
		return dst, fmt.Errorf("%w: array of %d dimensions", errBadValue, ndim)
	}
	if len(b) < 8 {
		return dst, fmt.Errorf("%w: truncated array", errBadValue)
	}
	n := int(int32(binary.BigEndian.Uint32(b))) // two's complement
	b = b[8:]                                   // length, lower bound
	for range n {
		if len(b) < 4 {
			return dst, fmt.Errorf("%w: truncated array", errBadValue)
		}
		l := int(int32(binary.BigEndian.Uint32(b))) // two's complement; -1: NULL
		b = b[4:]
		if l < 0 {
			dst = append(dst, nil)
			continue
		}
		if len(b) < l {
			return dst, fmt.Errorf("%w: truncated array element", errBadValue)
		}
		dst = append(dst, b[:l:l])
		b = b[l:]
	}
	return dst, nil
}

// interned holds the strings of the few values tasks repeat (workflow
// names, queues, statuses), so decoding a task does not copy them each
// time. Bounded: past maxInterned strings it stops adding.
var interned = struct {
	sync.RWMutex
	m map[string]string
}{m: map[string]string{}}

const maxInterned = 4096

// intern returns b as a string, shared with earlier equal ones.
func intern(b []byte) string {
	interned.RLock()
	s, ok := interned.m[string(b)] // a map lookup by string(b) does not copy b
	interned.RUnlock()
	if ok {
		return s
	}
	s = string(b)
	interned.Lock()
	if len(interned.m) < maxInterned {
		interned.m[s] = s
	}
	interned.Unlock()
	return s
}

// claimActivitiesFast is ClaimActivityTasks on the fast path (pollers
// claim often, mostly nothing: this keeps an empty claim cheap).
func (s *Store) claimActivitiesFast(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration, due bool) ([]store.ActivityTask, time.Duration, error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer pc.Release()
	fb := s.getFastBatch(ctx, pc.Conn())
	defer fb.release()
	fb.stmt(claimActivities, nClaimActivities, func() {
		fb.Strs(len(queues), func(i int) string { return queues[i] })
		fb.Str(owner)
		fb.Int(int64(limit))
		fb.Int(micros(ttl))
	})
	if due {
		fb.stmt(nextActivityDue, nNextActivityDue, func() {
			fb.Strs(len(queues), func(i int) string { return queues[i] })
		})
	}
	var out []store.ActivityTask
	var next time.Duration
	err = fb.exec(func(i int, v [][]byte) error {
		if i == 1 {
			return decodeDue(v, &next)
		}
		if len(v) != 14 {
			return fmt.Errorf("%w: %d activity task columns", errBadValue, len(v))
		}
		t := store.ActivityTask{Lease: store.Lease{Owner: owner}}
		var nums [5]int64
		for i, col := range [...]int{0, 1, 2, 4, 9} {
			n, err := decodeInt(v[col])
			if err != nil {
				return err
			}
			nums[i] = n
		}
		t.Lease.ID, t.Lease.Version, t.ExecutionID = nums[0], nums[1], nums[2]
		t.ScheduleID, t.Attempt = int32(nums[3]), int32(nums[4]) // int4 columns
		t.InstanceID, t.Name, t.Queue = string(v[3]), intern(v[5]), intern(v[6])
		t.Input = cloneBytes(v[7])
		t.Compensate = len(v[8]) == 1 && v[8][0] == 1
		t.CancelRequested = len(v[10]) == 1 && v[10][0] == 1
		t.Uncertain = len(v[11]) == 1 && v[11][0] == 1
		t.CompData = cloneBytes(v[12]) // NULL: nil, no allocation
		t.Trace = cloneBytes(v[13])    // NULL without a tracer: nil
		out = append(out, t)
		return nil
	})
	return out, next, err
}

// decodeDue decodes a next*Due row (microseconds; NULL: none, 0).
func decodeDue(v [][]byte, next *time.Duration) error {
	if len(v) != 1 {
		return fmt.Errorf("%w: %d due columns", errBadValue, len(v))
	}
	if v[0] == nil {
		return nil
	}
	us, err := decodeInt(v[0])
	*next = time.Duration(us) * time.Microsecond
	return err
}
