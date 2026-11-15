package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Statement names of the fast path (prepared once per connection).
var (
	nFence          = stmtName(fence)
	nNoUndelivered  = stmtName(noUndelivered)
	nInsertHistory  = stmtName(insertHistory)
	nDeletePending  = stmtName(deletePending)
	nInsertPending  = stmtName(insertPending)
	nInsertActs     = stmtName(insertActivities)
	nInsertChild    = stmtName(insertChild)
	nCancelChildren = stmtName(cancelChildren)
	nCancelActs     = stmtName(cancelActivities)
	nAcquireLock    = stmtName(acquireLock)
	nUpdateRunning  = stmtName(updateRunning)
	nEndExecution   = stmtName(endExecution)
	nContinueAsNew  = stmtName(continueAsNew)
	nCarryPending   = stmtName(carryPending)
	nDropEnded      = stmtName(dropEnded)
	nNotifyParent   = stmtName(notifyParent)
	nWakeForTask    = stmtName(wakeForTask)
	nFinishTask     = stmtName(finishTask)
	nResumeExtend   = stmtName(resumeExtend)
	nResumeWorkflow = stmtName(resumeWorkflow)
	nResumePending  = stmtName(resumePending)
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
func (s *Store) completeFast(ctx context.Context, l store.Lease, c *store.Completion, ending, terminal bool,
	prevSeq, lastSeq int64,
) (map[actKey]int64, error) {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer pc.Release()
	fb := getFastBatch(ctx, pc.Conn())
	defer fb.release()

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
	for i := range c.Children {
		ch := &c.Children[i]
		fb.stmt(insertChild, nInsertChild, func() {
			fb.Str(ch.InstanceID)
			fb.Str(ch.Workflow)
			fb.Int(int64(ch.Version))
			fb.Str(ch.Queue)
			fb.Int(l.ID)
			fb.Int(int64(ch.ParentSched))
			fb.Bytes(ch.Input)
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
	locks := sortedLocks(c.Locks)
	for i := range locks {
		k := &locks[i]
		fb.stmt(acquireLock, nAcquireLock, func() {
			fb.Int(l.ID)
			fb.Str(k.Key)
			fb.Int(int64(k.ScheduleID))
			fb.Int(micros(k.TTL))
		})
	}
	s.endFast(fb, l, c, ending, lastSeq)

	var claimed map[actKey]int64
	err = fb.exec(func(i int, rr *pgconn.ResultReader) error {
		if i != activityAt {
			return nil
		}
		for rr.NextRow() {
			v := rr.Values()
			id, err := decodeInt(v[0])
			if err != nil {
				return err
			}
			sched, err := decodeInt(v[1])
			if err != nil {
				return err
			}
			if len(v[3]) > 0 { // owner_id: claimed eagerly
				if claimed == nil {
					claimed = make(map[actKey]int64, len(c.Activities))
				}
				claimed[actKey{sched: int32(sched), comp: len(v[2]) == 1 && v[2][0] == 1}] = id // int4 column
			}
		}
		return nil
	})
	return claimed, err
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
		})
		fb.stmt(carryPending, nCarryPending, func() { fb.Int(l.ID) })
	}
	fb.stmt(dropEnded, nDropEnded, func() { fb.Int(l.ID) })
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
	fb := getFastBatch(ctx, pc.Conn())
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
	var task store.WorkflowTask
	found := false
	err = fb.exec(func(i int, rr *pgconn.ResultReader) error {
		switch i {
		case 1: // finishTask: the execution id
			for rr.NextRow() {
				var err error
				if execID, err = decodeInt(rr.Values()[0]); err != nil {
					return err
				}
			}
		case 3: // resumeWorkflow
			for rr.NextRow() {
				if err := decodeTask(rr.Values(), &task, r.Resume.Owner); err != nil {
					return err
				}
				found = true
			}
		case 4: // resumePending
			for rr.NextRow() {
				p, err := decodePending(rr.Values())
				if err != nil {
					return err
				}
				task.Pending = append(task.Pending, p)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if r.Resume == nil || !found || task.Execution.ID != execID {
		return nil, nil
	}
	return &task, nil
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

// decodeTask decodes executionColumns + lease_version, last_seq, now().
func decodeTask(v [][]byte, t *store.WorkflowTask, owner string) error {
	if len(v) != 17 {
		return fmt.Errorf("%w: %d task columns", errBadValue, len(v))
	}
	e := &t.Execution
	var err error
	ints := [...]*int64{&e.ID, &e.ParentID, &t.Lease.Version, &t.LastSeq}
	for i, col := range [...]int{0, 6, 14, 15} {
		if *ints[i], err = decodeInt(v[col]); err != nil {
			return err
		}
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
	e.InstanceID, e.Workflow, e.Queue = string(v[1]), string(v[2]), string(v[4])
	e.Status = store.Status(v[5])
	e.Input, e.Output = cloneBytes(v[8]), cloneBytes(v[9])
	e.Err, e.ErrKinds = string(v[10]), string(v[11])
	if e.CreatedAt, err = decodeTime(v[12]); err != nil {
		return err
	}
	if e.EndedAt, err = decodeTime(v[13]); err != nil {
		return err
	}
	if t.Now, err = decodeTime(v[16]); err != nil {
		return err
	}
	t.Lease.ID, t.Lease.Owner = e.ID, owner
	return nil
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
