package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
)

// Store is the Postgres store.
//
// Round trips: a claim, a resume, a workflow task completion and an
// activity completion (with resume) each take one round trip, plus one the
// first time a connection prepares each statement. Multi-statement
// operations are sent as one pgx batch, which Postgres runs as one implicit
// transaction (one Sync), so they apply atomically.
//
// Failures inside a batch are raised by the fence statement with volatile
// expressions that fail at run time (never at plan time) and are mapped back
// to errors by SQLSTATE:
//   - 22012 (division by zero): the lease was lost;
//   - 22P02 (invalid input): the history is out of sequence;
//   - 2201E (log of zero): undelivered signals or cancel requests are
//     pending.
type Store struct {
	pool *pgxpool.Pool
	fast bool // hot paths on pgconn (fast.go)
}

var _ store.Store = (*Store)(nil)

// New returns a store on pool (migrated with Migrate).
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool, fast: fastPathEnabled(pool)} }

const (
	codeLeaseLost    = "22012"
	codeOutOfSeq     = "22P02"
	codePending      = "2201E"
	codeDeadlock     = "40P01"
	codeSerializable = "40001"
	maxRetries       = 5
)

// activeStatuses is the SQL list of statuses that hold an instance id.
const activeStatuses = `('running', 'compensating', 'needs_attention')`

// micros converts a duration for "$n * interval '1 microsecond'": the
// String form ("1.5µs") is not always valid Postgres interval input.
func micros(d time.Duration) int64 { return d.Microseconds() }

func sqlState(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}

// mapErr turns the errors raised by fence statements into store errors.
func mapErr(err error) error {
	switch sqlState(err) {
	case codeLeaseLost:
		return store.ErrLeaseLost
	case codeOutOfSeq:
		return store.ErrInvalidCompletion
	case codePending:
		return store.ErrPendingEvents
	}
	return err
}

// nonNil returns s, or an empty slice for nil: pgx sends a nil slice as
// NULL, and "x <> ALL(NULL)" is NULL, not true.
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// retry runs fn again when Postgres aborted it for a deadlock or a
// serialization failure, or when a prepared statement went stale (after a
// migration the connection is closed and the next one prepares afresh),
// after a short randomized backoff. fn must be
// all-or-nothing (one batch or one statement): a retry starts from scratch.
//
// Known deadlock: a parent cancelling a child (parent row, then child row)
// while that child ends and notifies the parent (child row, then parent
// row). Postgres detects it after deadlock_timeout (1s by default) and the
// retry succeeds; it is rare and only costs latency.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := range maxRetries {
		if err = fn(); err == nil {
			return nil
		}
		if s := sqlState(err); s != codeDeadlock && s != codeSerializable && !staleStatement(err) {
			return err
		}
		backoff := time.Duration(1+rand.IntN(5<<attempt)) * time.Millisecond //nolint:gosec // jitter, not security
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}

const executionColumns = `id, instance_id, workflow, version, queue, status, parent_id, parent_sched,
	input, output, err, err_kinds, created_at, ended_at`

func scanExecution(row pgx.Row, extra ...any) (store.Execution, error) {
	var e store.Execution
	var status string
	var ended pgtype.Timestamptz
	dest := append([]any{
		&e.ID, &e.InstanceID, &e.Workflow, &e.Version, &e.Queue, &status, &e.ParentID, &e.ParentSched,
		&e.Input, &e.Output, &e.Err, &e.ErrKinds, &e.CreatedAt, &ended,
	}, extra...)
	if err := row.Scan(dest...); err != nil {
		return e, err
	}
	e.Status = store.Status(status)
	if ended.Valid {
		e.EndedAt = ended.Time
	}
	return e, nil
}

// eventColumns are the event columns shared by history and pending_event.
const eventColumns = `type, schedule_id, attempt, name, version, ref, queue, arg_hash, payload, comp_data,
	err, err_kinds, uncertain, permanent, at`

func eventDest(ev *history.Event, at *pgtype.Timestamptz, argHash *int64) []any {
	return []any{
		&ev.Type, &ev.ScheduleID, &ev.Attempt, &ev.Name, &ev.Version, &ev.Ref, &ev.Queue, argHash,
		&ev.Payload, &ev.CompData, &ev.Err, &ev.ErrKinds, &ev.Uncertain, &ev.Permanent, at,
	}
}

func finishEvent(ev *history.Event, at pgtype.Timestamptz, argHash int64) {
	ev.ArgHash = uint64(argHash) // bigint holds the same 64 bits
	if at.Valid {
		ev.At = at.Time
	}
}

func tstz(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: !t.IsZero()} }

// eventArgs are the event columns as arrays, for INSERT ... SELECT FROM
// unnest(...).
type eventArgs struct {
	types                      []int16
	scheds, attempts, versions []int32
	names, refs, queues        []string
	errs, kinds                []string
	hashes                     []int64
	payloads, comps            [][]byte
	uncertain, permanent       []bool
	ats, visible               []pgtype.Timestamptz
	seqs                       []int64
}

func (a *eventArgs) add(ev *history.Event, visible time.Time) {
	a.types = append(a.types, int16(ev.Type))
	a.scheds = append(a.scheds, ev.ScheduleID)
	a.attempts = append(a.attempts, ev.Attempt)
	a.versions = append(a.versions, ev.Version)
	a.names = append(a.names, ev.Name)
	a.refs = append(a.refs, ev.Ref)
	a.queues = append(a.queues, ev.Queue)
	a.errs = append(a.errs, ev.Err)
	a.kinds = append(a.kinds, ev.ErrKinds)
	a.hashes = append(a.hashes, int64(ev.ArgHash)) // same 64 bits in a bigint
	a.payloads = append(a.payloads, ev.Payload)
	a.comps = append(a.comps, ev.CompData)
	a.uncertain = append(a.uncertain, ev.Uncertain)
	a.permanent = append(a.permanent, ev.Permanent)
	a.ats = append(a.ats, tstz(ev.At))
	a.visible = append(a.visible, tstz(visible))
	a.seqs = append(a.seqs, ev.Seq)
}

const unnestEvents = `unnest($2::smallint[], $3::int[], $4::int[], $5::text[], $6::int[], $7::text[], $8::text[],
	$9::bigint[], $10::bytea[], $11::bytea[], $12::text[], $13::text[], $14::boolean[], $15::boolean[],
	$16::timestamptz[], $17::bigint[], $18::timestamptz[])
	AS u(type, schedule_id, attempt, name, version, ref, queue, arg_hash, payload, comp_data, err, err_kinds,
	     uncertain, permanent, at, seq, visible_at)`

func (a *eventArgs) args(executionID int64) []any {
	return []any{
		executionID, a.types, a.scheds, a.attempts, a.names, a.versions, a.refs, a.queues, a.hashes,
		a.payloads, a.comps, a.errs, a.kinds, a.uncertain, a.permanent, a.ats, a.seqs, a.visible,
	}
}

// Now implements store.Store.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return now, fmt.Errorf("now: %w", err)
	}
	return now, nil
}

// insertExecution inserts an execution and its start event unless the
// instance id exists (any status) or an active execution of it is being
// inserted concurrently. $8 = 0 for a root execution.
const insertExecution = `
	WITH ins AS (
		INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at)
		SELECT $1, $2, $3, $4, 'running', $5, $6, $7, now()
		WHERE NOT EXISTS (SELECT 1 FROM flows.execution WHERE instance_id = $1)
		ON CONFLICT (instance_id) WHERE status IN ` + activeStatuses + ` DO NOTHING
		RETURNING ` + executionColumns + `
	), start AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, version, queue, payload)
		SELECT id, now(), ` + typeStarted + `, $2, $3, $4, $7 FROM ins
		RETURNING ` + notifyW + "$4)" + `
	)
	SELECT ` + executionColumns + ` FROM ins`

const typeStarted = "1" // history.ExecutionStarted

// CreateExecution implements store.Store.
func (s *Store) CreateExecution(ctx context.Context, ne store.NewExecution) (store.Execution, bool, error) {
	for range maxRetries {
		e, err := scanExecution(s.pool.QueryRow(ctx, insertExecution,
			ne.InstanceID, ne.Workflow, ne.Version, ne.Queue, ne.ParentID, ne.ParentSched, ne.Input))
		if err == nil {
			return e, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return e, false, fmt.Errorf("create execution: %w", err)
		}
		e, err = s.GetExecution(ctx, ne.InstanceID)
		if errors.Is(err, store.ErrNotFound) {
			continue // the concurrent insert we waited for rolled back
		}
		if err != nil {
			return e, false, err
		}
		if e.Workflow != ne.Workflow {
			return store.Execution{}, false, store.ErrInstanceConflict
		}
		return e, false, nil
	}
	return store.Execution{}, false, errors.New("create execution: instance id kept changing hands")
}

// GetExecution implements store.Store.
func (s *Store) GetExecution(ctx context.Context, instanceID string) (store.Execution, error) {
	e, err := scanExecution(s.pool.QueryRow(ctx, `SELECT `+executionColumns+`
		FROM flows.execution WHERE instance_id = $1 ORDER BY id DESC LIMIT 1`, instanceID))
	if errors.Is(err, pgx.ErrNoRows) {
		return e, store.ErrNotFound
	}
	if err != nil {
		return e, fmt.Errorf("get execution: %w", err)
	}
	return e, nil
}

// claimWorkflow grants leases on up to $3 ready executions of the queues $1
// to owner $2 for $4 microseconds (GW-8: index scan in readiness order).
const claimWorkflow = `
	WITH c AS (
		SELECT id FROM flows.execution
		WHERE queue = ANY($1) AND status IN ('running', 'compensating')
		  AND ready_at <= now()
		  AND (locked_until IS NULL OR locked_until < now())
		  AND (not_before IS NULL OR not_before <= now())
		  AND (sticky_until IS NULL OR sticky_until < now() OR sticky_owner = $2)
		ORDER BY ready_at, id
		LIMIT $3
		FOR NO KEY UPDATE SKIP LOCKED
	)
	, k AS (
		-- The locks of a running execution do not lapse (store.Completion.Locks).
		-- It waits for a lock row a contender holds (acquireLock never waits
		-- on an execution row: no cycle) and re-checks its holder.
		UPDATE flows.lock k SET until = now() + k.ttl_us * interval '1 microsecond'
		FROM c WHERE k.execution_id = c.id
	)
	UPDATE flows.execution e
	SET owner_id = $2, lease_version = e.lease_version + 1,
	    locked_until = now() + $4 * interval '1 microsecond', not_before = NULL
	FROM c WHERE e.id = c.id
	RETURNING e.id, e.instance_id, e.workflow, e.version, e.queue, e.status, e.parent_id, e.parent_sched,
	          e.input, e.output, e.err, e.err_kinds, e.created_at, e.ended_at,
	          e.lease_version, e.last_seq, now()`

// claimedPending returns the visible pending events of the executions
// leased by this transaction: in the same implicit transaction as
// claimWorkflow, now() is the same, so locked_until identifies them.
const claimedPending = `
	SELECT p.id, p.execution_id, ` + pendingColumnsP + `
	FROM flows.pending_event p
	JOIN flows.execution e ON e.id = p.execution_id
	WHERE e.owner_id = $1 AND e.locked_until = now() + $2 * interval '1 microsecond'
	  AND p.visible_at <= now()
	ORDER BY p.execution_id, p.visible_at, p.id`

const pendingColumnsP = `p.type, p.schedule_id, p.attempt, p.name, p.version, p.ref, p.queue, p.arg_hash,
	p.payload, p.comp_data, p.err, p.err_kinds, p.uncertain, p.permanent, p.at`

// ClaimWorkflowTasks implements store.Store.
func (s *Store) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	var tasks []store.WorkflowTask
	err := retry(ctx, func() error {
		tasks = tasks[:0]
		b := &pgx.Batch{}
		b.Queue(claimWorkflow, queues, owner, limit, micros(ttl))
		b.Queue(claimedPending, owner, micros(ttl))
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		var err error
		if tasks, err = readTasks(br, owner); err != nil {
			return err
		}
		if err := readPending(br, tasks); err != nil {
			return err
		}
		return br.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("claim workflow tasks: %w", err)
	}
	return tasks, nil
}

func readTasks(br pgx.BatchResults, owner string) ([]store.WorkflowTask, error) {
	rows, err := br.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []store.WorkflowTask
	for rows.Next() {
		var t store.WorkflowTask
		e, err := scanExecution(rows, &t.Lease.Version, &t.LastSeq, &t.Now)
		if err != nil {
			return nil, err
		}
		t.Execution = e
		t.Lease.ID, t.Lease.Owner = e.ID, owner
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// readPending distributes pending rows (id, execution_id, event columns)
// onto tasks.
func readPending(br pgx.BatchResults, tasks []store.WorkflowTask) error {
	rows, err := br.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	byID := make(map[int64]int, len(tasks))
	for i, t := range tasks {
		byID[t.Execution.ID] = i
	}
	for rows.Next() {
		var p store.PendingEvent
		var execID, hash int64
		var at pgtype.Timestamptz
		if err := rows.Scan(append([]any{&p.ID, &execID}, eventDest(&p.Event, &at, &hash)...)...); err != nil {
			return err
		}
		finishEvent(&p.Event, at, hash)
		if i, ok := byID[execID]; ok {
			tasks[i].Pending = append(tasks[i].Pending, p)
		}
	}
	return rows.Err()
}

// LoadHistory implements store.Store.
func (s *Store) LoadHistory(ctx context.Context, executionID, afterSeq int64) ([]history.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, `+eventColumns+` FROM flows.history
		WHERE execution_id = $1 AND seq > $2 ORDER BY seq`, executionID, afterSeq)
	if err != nil {
		return nil, fmt.Errorf("load history: %w", err)
	}
	defer rows.Close()
	var out []history.Event
	for rows.Next() {
		var ev history.Event
		var hash int64
		var at pgtype.Timestamptz
		if err := rows.Scan(append([]any{&ev.Seq}, eventDest(&ev, &at, &hash)...)...); err != nil {
			return nil, fmt.Errorf("load history: %w", err)
		}
		finishEvent(&ev, at, hash)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load history: %w", err)
	}
	if len(out) == 0 {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM flows.execution WHERE id = $1)`, executionID).Scan(&exists); err != nil {
			return nil, fmt.Errorf("load history: %w", err)
		}
		if !exists {
			return nil, store.ErrNotFound
		}
	}
	return out, nil
}

// ExtendWorkflowTask implements store.Store.
func (s *Store) ExtendWorkflowTask(ctx context.Context, l store.Lease, ttl time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE flows.execution SET locked_until = now() + $4 * interval '1 microsecond'
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''`,
		l.ID, l.Owner, l.Version, micros(ttl))
	if err != nil {
		return fmt.Errorf("extend workflow task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrLeaseLost
	}
	return nil
}

// ReleaseWorkflowTask implements store.Store.
//
// The fence locks the row first, so that releaseWorkflow computes ready_at
// from a snapshot taken after the lock: it includes every event delivered
// before, and later deliveries wait for the lock (see wakeForTask). A
// single UPDATE would compute it from a snapshot taken before waiting for
// the lock and could miss one, leaving the execution unclaimable.
func (s *Store) ReleaseWorkflowTask(ctx context.Context, l store.Lease, delay time.Duration) error {
	err := retry(ctx, func() error {
		b := &pgx.Batch{}
		b.Queue(fence, l.ID, l.Owner, l.Version, int64(0), false)
		b.Queue(releaseWorkflow, l.ID, micros(delay))
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		var seq int64
		if err := br.QueryRow().Scan(&seq); err != nil {
			return err
		}
		if _, err := br.Exec(); err != nil {
			return err
		}
		return br.Close()
	})
	if err = mapErr(err); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return fmt.Errorf("release workflow task: %w", err)
	}
	return err
}

const releaseWorkflow = `
	UPDATE flows.execution
	SET owner_id = '', locked_until = NULL, sticky_owner = '', sticky_until = NULL,
	    ready_at = (SELECT min(visible_at) FROM flows.pending_event WHERE execution_id = $1),
	    not_before = CASE WHEN $2::bigint > 0 THEN now() + $2::bigint * interval '1 microsecond' END
	WHERE id = $1
	RETURNING CASE WHEN $2::bigint = 0 AND ready_at <= now() THEN ` + notifyW + "queue)" + ` END`

// resumeExtend extends a kept lease, but only when less than half of ttl
// is left: in the eager loop the lease was just set by the completion, and
// skipping the write avoids a non-HOT update of the execution row (it is in
// indexes through ready_at). resumeWorkflow then reads the execution if the
// lease is still held, and resumePending its visible pending events.
const (
	resumeExtend = `
		UPDATE flows.execution
		SET locked_until = now() + $4::bigint * interval '1 microsecond',
		    ready_at = least(ready_at, now() + $4::bigint * interval '1 microsecond')
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
		  AND locked_until < now() + ($4::bigint / 2) * interval '1 microsecond'`
	resumeWorkflow = `
		SELECT ` + executionColumns + `, lease_version, last_seq, now()
		FROM flows.execution
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
		  AND status IN ('running', 'compensating')`
	resumePending = `
		SELECT p.id, p.execution_id, ` + pendingColumnsP + `
		FROM flows.pending_event p
		JOIN flows.execution e ON e.id = p.execution_id
		WHERE e.id = $1 AND e.owner_id = $2 AND e.lease_version = $3 AND e.owner_id <> ''
		  AND p.visible_at <= now()
		ORDER BY p.visible_at, p.id`
)

func queueResume(b *pgx.Batch, l store.Lease, ttl time.Duration) {
	b.Queue(resumeExtend, l.ID, l.Owner, l.Version, micros(ttl))
	b.Queue(resumeWorkflow, l.ID, l.Owner, l.Version)
	b.Queue(resumePending, l.ID, l.Owner, l.Version)
}

// readResume reads the results of queueResume; ok is false when the lease
// was lost.
func readResume(br pgx.BatchResults, owner string) (store.WorkflowTask, bool, error) {
	if _, err := br.Exec(); err != nil {
		return store.WorkflowTask{}, false, err
	}
	tasks, err := readTasks(br, owner)
	if err != nil {
		return store.WorkflowTask{}, false, err
	}
	if err := readPending(br, tasks); err != nil {
		return store.WorkflowTask{}, false, err
	}
	if len(tasks) == 0 {
		return store.WorkflowTask{}, false, nil
	}
	return tasks[0], true, nil
}

// ResumeWorkflowTask implements store.Store.
func (s *Store) ResumeWorkflowTask(ctx context.Context, l store.Lease, ttl time.Duration) (store.WorkflowTask, error) {
	b := &pgx.Batch{}
	queueResume(b, l, ttl)
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	t, ok, err := readResume(br, l.Owner)
	if err == nil {
		err = br.Close()
	}
	if err != nil {
		return t, fmt.Errorf("resume workflow task: %w", err)
	}
	if !ok {
		return t, store.ErrLeaseLost
	}
	return t, nil
}

// fence locks the execution row under lease ($1 id, $2 owner, $3 version)
// and raises (see Store) when the lease is lost or when $5 (history is
// appended) and last_seq differs from $4. random() keeps the failing expressions out of
// constant folding.
const fence = `
	SELECT CASE
		WHEN f.last_seq IS NULL THEN (1 / (0 * random()))::bigint
		WHEN $5::boolean AND f.last_seq <> $4::bigint THEN ('out of sequence' || random())::bigint
		ELSE f.last_seq END
	FROM (SELECT 1) one
	LEFT JOIN LATERAL (
		SELECT last_seq FROM flows.execution
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
		FOR NO KEY UPDATE
	) f ON true`

// noUndelivered raises when signals or cancel requests not in $2 are
// pending. It must be a statement of its own after the fence: in READ
// COMMITTED its snapshot is taken after the row lock is held, so it sees
// every signal that committed before (later ones wait on the row lock and
// then find the execution ended). Inside the fence statement, the snapshot
// would predate the lock wait and miss them (GW-3).
const noUndelivered = `
	SELECT CASE WHEN EXISTS (
		SELECT 1 FROM flows.pending_event p
		WHERE p.execution_id = $1 AND p.type IN (` + typeCancelRequested + `, ` + typeSignal + `, ` + typeLockLost + `)
		  AND p.id <> ALL($2::bigint[]))
	THEN ln(0 * random())::int ELSE 0 END`

const (
	typeCancelRequested = "7"  // history.ExecutionCancelRequested
	typeTimerFired      = "14" // history.TimerFired
	typeSignal          = "16" // history.SignalReceived
	typeChildFailed     = "20" // history.ChildFailed
)

const insertHistory = `
	INSERT INTO flows.history (execution_id, seq, ` + eventColumns + `)
	SELECT $1, seq, type, schedule_id, attempt, name, version, ref, queue, arg_hash, payload, comp_data,
	       err, err_kinds, uncertain, permanent, at
	FROM ` + unnestEvents

const insertPending = `
	INSERT INTO flows.pending_event (execution_id, visible_at, ` + eventColumns + `)
	SELECT $1, visible_at, type, schedule_id, attempt, name, version, ref, queue, arg_hash, payload, comp_data,
	       err, err_kinds, uncertain, permanent, at
	FROM ` + unnestEvents

const deletePending = `
	DELETE FROM flows.pending_event
	WHERE execution_id = $1
	  AND (id = ANY($2::bigint[]) OR (type = ` + typeTimerFired + ` AND schedule_id = ANY($3::int[])))`

// insertActivities creates activity tasks; those with a claim duration are
// created leased to $8 (eager execution).
const insertActivities = `
	INSERT INTO flows.activity_task (execution_id, instance_id, schedule_id, compensate, name, queue, input,
	                                 attempt, owner_id, lease_version, locked_until, visible_at, uncertain)
	SELECT $1, e.instance_id, u.sched, u.comp, u.name, u.queue, u.input,
	       CASE WHEN u.claim > 0 THEN 1 ELSE 0 END,
	       CASE WHEN u.claim > 0 THEN $8 ELSE '' END,
	       CASE WHEN u.claim > 0 THEN 1 ELSE 0 END,
	       CASE WHEN u.claim > 0 THEN now() + u.claim * interval '1 microsecond' END,
	       now() + greatest(u.claim, 0) * interval '1 microsecond', u.uncertain
	FROM flows.execution e,
	     unnest($2::int[], $3::boolean[], $4::text[], $5::text[], $6::bytea[], $7::bigint[], $9::boolean[])
	       AS u(sched, comp, name, queue, input, claim, uncertain)
	WHERE e.id = $1
	RETURNING id, schedule_id, compensate, owner_id,
	          CASE WHEN owner_id = '' THEN ` + notifyA + "queue)" + ` END`

// acquireLock answers lock request $2 (schedule id $3, ttl $4 µs) of
// execution $1 with a pending LockAcquired or LockHeld; taking over an
// expired lock of another active execution delivers LockLost to it. The
// conflict clause re-checks the row a concurrent request may have created.
const acquireLock = `
	WITH cur AS (
		SELECT execution_id, sched, until > now() AS live FROM flows.lock WHERE key = $2 FOR UPDATE
	), prev AS (
		-- The expired holder's row, unless it runs: its row is locked (being
		-- claimed or completed), its lease is live, or one of its activities
		-- runs. Then the lock stays its own (its renewals extend it).
		SELECT x.id, x.queue, x.locked_until, cur.sched FROM flows.execution x, cur
		WHERE x.id = cur.execution_id AND cur.execution_id <> $1 AND NOT cur.live
		  AND (x.locked_until IS NULL OR x.locked_until <= now())
		  AND NOT EXISTS (SELECT 1 FROM flows.activity_task t
		                  WHERE t.execution_id = x.id AND t.owner_id <> '' AND t.locked_until > now())
		FOR NO KEY UPDATE OF x SKIP LOCKED
	), take AS (
		INSERT INTO flows.lock AS k (key, execution_id, sched, until, ttl_us)
		SELECT $2, $1, $3, now() + $4::bigint * interval '1 microsecond', $4
		WHERE NOT EXISTS (SELECT 1 FROM cur WHERE execution_id <> $1 AND (live OR NOT EXISTS (SELECT 1 FROM prev)))
		ON CONFLICT (key) DO UPDATE
		SET execution_id = excluded.execution_id, sched = excluded.sched, until = excluded.until, ttl_us = excluded.ttl_us
		WHERE k.until <= now() OR k.execution_id = $1
		RETURNING 1
	), lost AS (
		UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
		FROM prev
		WHERE x.id = prev.id AND EXISTS (SELECT 1 FROM take) AND x.status IN ` + activeStatuses + `
		RETURNING x.id, prev.sched,
		          CASE WHEN x.locked_until IS NULL OR x.locked_until <= now() THEN ` + notifyW + "x.queue)" + ` END AS n
	), lost_event AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, name)
		SELECT id, now(), ` + typeLockLost + `, sched, $2 FROM lost
	)
	INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, name, err)
	SELECT $1, now(),
	       CASE WHEN EXISTS (SELECT 1 FROM take) THEN ` + typeLockAcquired + ` ELSE ` + typeLockHeld + ` END, $3, $2,
	       CASE WHEN EXISTS (SELECT 1 FROM take) THEN ''
	            ELSE coalesce((SELECT e.instance_id FROM cur JOIN flows.execution e ON e.id = cur.execution_id),
	                          'another execution') END`

// sortedLocks orders lock requests by key: two executions taking the same
// locks in one checkpoint lock their rows in the same order (no deadlock).
// Answers go by schedule id, so the order is invisible.
func sortedLocks(locks []store.LockRequest) []store.LockRequest {
	if len(locks) < 2 {
		return locks
	}
	s := slices.Clone(locks)
	slices.SortFunc(s, func(a, b store.LockRequest) int { return strings.Compare(a.Key, b.Key) })
	return s
}

const (
	typeLockAcquired = "27" // history.LockAcquired
	typeLockHeld     = "28" // history.LockHeld
	typeLockLost     = "29" // history.LockLost
)

// insertChild creates a child execution, or delivers ChildFailed
// (instance_exists) to the parent $5 when the id is taken.
const insertChild = `
	WITH ins AS (
		INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at)
		SELECT $1, $2, $3, $4, 'running', $5, $6, $7, now()
		WHERE NOT EXISTS (SELECT 1 FROM flows.execution WHERE instance_id = $1)
		ON CONFLICT (instance_id) WHERE status IN ` + activeStatuses + ` DO NOTHING
		RETURNING id
	), start AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, version, queue, payload)
		SELECT id, now(), ` + typeStarted + `, $2, $3, $4, $7 FROM ins
		RETURNING ` + notifyW + "$4)" + `
	)
	INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, ref, err, err_kinds, permanent)
	SELECT $5, now(), ` + typeChildFailed + `, $6, $1, 'child instance id already in use', '` + store.KindInstanceExists + `', true
	WHERE NOT EXISTS (SELECT 1 FROM ins)`

const cancelChildren = `
	WITH t AS (
		UPDATE flows.execution SET ready_at = least(coalesce(ready_at, now()), now())
		WHERE instance_id = ANY($2::text[]) AND parent_id = $1 AND status IN ` + activeStatuses + `
		RETURNING id, queue
	), ins AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type)
		SELECT id, now(), ` + typeCancelRequested + ` FROM t
	)
	SELECT ` + notifyW + "queue) FROM t"

// cancelActivities marks the tasks of activities the workflow canceled;
// RenewLeases reports them to the workers running them.
const cancelActivities = `
	UPDATE flows.activity_task
	SET cancel_requested = true,
	    visible_at = CASE WHEN owner_id = '' THEN least(visible_at, now()) ELSE visible_at END
	WHERE execution_id = $1 AND schedule_id = ANY($2::int[]) AND NOT compensate`

// updateRunning closes a task that leaves the execution active: $2 last
// seq (0: unchanged), $3 status (empty: unchanged), $4 keep-for, $5
// sticky-for, $6 owner, $7/$8 the reason of a park (needs_attention) or of
// a compensation (the workflow's error).
// The lease is kept only while the execution stays
// claimable. Released with work visible, it notifies other owners.
const updateRunning = `
	WITH v AS (
		SELECT coalesce(nullif($3::text, ''), status) AS status,
		       $4::bigint > 0 AND coalesce(nullif($3::text, ''), status) IN ('running', 'compensating') AS keep
		FROM flows.execution WHERE id = $1
	)
	UPDATE flows.execution e
	SET last_seq = CASE WHEN $2::bigint > 0 THEN $2::bigint ELSE e.last_seq END,
	    status = v.status,
	    err = CASE WHEN $3::text IN ('needs_attention', 'compensating') THEN $7::text ELSE e.err END,
	    err_kinds = CASE WHEN $3::text IN ('needs_attention', 'compensating') THEN $8::text ELSE e.err_kinds END,
	    ready_at = CASE WHEN v.keep
	        THEN least((SELECT min(visible_at) FROM flows.pending_event WHERE execution_id = $1),
	                   now() + $4 * interval '1 microsecond')
	        ELSE (SELECT min(visible_at) FROM flows.pending_event WHERE execution_id = $1) END,
	    owner_id = CASE WHEN v.keep THEN e.owner_id ELSE '' END,
	    locked_until = CASE WHEN v.keep THEN now() + $4 * interval '1 microsecond' END,
	    sticky_owner = CASE WHEN NOT v.keep AND $5::bigint > 0 THEN $6 ELSE '' END,
	    sticky_until = CASE WHEN NOT v.keep AND $5::bigint > 0 THEN now() + $5 * interval '1 microsecond' END
	FROM v WHERE e.id = $1
	RETURNING CASE WHEN NOT v.keep AND e.ready_at <= now() THEN ` + notifyW + "e.queue)" + ` END`

// endExecution ends an execution; continueAsNew then creates the next one
// and carryPending moves the undelivered signals and cancel requests to it.
const (
	endExecution = `
		UPDATE flows.execution
		SET last_seq = CASE WHEN $2::bigint > 0 THEN $2::bigint ELSE last_seq END,
		    status = $3, output = $4, err = $5, err_kinds = $6, ended_at = now(),
		    ready_at = NULL, owner_id = '', locked_until = NULL, sticky_owner = '', sticky_until = NULL
		WHERE id = $1`
	continueAsNew = `
		WITH old AS (
			SELECT instance_id, parent_id, parent_sched FROM flows.execution WHERE id = $1
		), ins AS (
			INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at)
			SELECT old.instance_id, $2, $3, $4, 'running', old.parent_id, old.parent_sched, $5, now() FROM old
			RETURNING id
		)
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, version, queue, payload)
		SELECT id, now(), ` + typeStarted + `, $2, $3, $4, $5 FROM ins
		RETURNING ` + notifyW + "$4)"
	// carryPending copies the undelivered signals and cancel requests, in
	// order, to the new execution (after its start event); dropEnded then
	// deletes the originals.
	carryPending = `
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, payload)
		SELECT n.id, now(), p.type, p.name, p.payload
		FROM flows.pending_event p,
		     (SELECT id FROM flows.execution
		      WHERE instance_id = (SELECT instance_id FROM flows.execution WHERE id = $1)
		        AND status = 'running' ORDER BY id DESC LIMIT 1) n
		WHERE p.execution_id = $1 AND p.type IN (` + typeCancelRequested + `, ` + typeSignal + `)
		ORDER BY p.visible_at, p.id`
	// dropEnded removes what an ended execution can no longer use: its
	// pending events and its activity tasks, so their workers learn of the
	// end at their next heartbeat (ErrLeaseLost) instead of running on, and
	// its business locks. No lock-order cycle: activity completions lock the
	// execution row before the task row (wakeForTask); claims and renewals
	// lock execution or task rows, then lock rows with SKIP LOCKED; a lock
	// takeover (acquireLock) locks a lock row, then the previous holder's
	// execution row with SKIP LOCKED.
	dropEnded = `
		WITH p AS (DELETE FROM flows.pending_event WHERE execution_id = $1),
		     k AS (DELETE FROM flows.lock WHERE execution_id = $1)
		DELETE FROM flows.activity_task WHERE execution_id = $1`
	// notifyParent delivers the child's outcome ($2..$7) to its active
	// parent.
	notifyParent = `
		WITH c AS (
			SELECT parent_id, parent_sched, instance_id FROM flows.execution WHERE id = $1 AND parent_id <> 0
		), p AS (
			UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
			FROM c WHERE x.id = c.parent_id AND x.status IN ` + activeStatuses + `
			RETURNING x.id, x.queue
		), ins AS (
			INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, ref, payload, err, err_kinds)
			SELECT p.id, now(), $2, c.parent_sched, c.instance_id, $3, $4, $5 FROM p, c
		)
		SELECT ` + notifyW + "queue) FROM p"
)

func validateCompletion(c *store.Completion) error {
	for i := 1; i < len(c.History); i++ {
		if c.History[i].Seq != c.History[i-1].Seq+1 {
			return store.ErrInvalidCompletion
		}
	}
	if c.ContinueAsNew != nil && c.Status != store.StatusContinued {
		return store.ErrInvalidCompletion
	}
	ending := c.Status != "" && !c.Status.Active()
	if ending && (len(c.Activities) > 0 || len(c.Timers) > 0 || len(c.Children) > 0 || len(c.Locks) > 0) {
		return store.ErrInvalidCompletion
	}
	for _, k := range c.Locks {
		if k.TTL <= 0 {
			return store.ErrInvalidCompletion
		}
	}
	type key struct {
		sched int32
		comp  bool
	}
	seen := make(map[key]bool, len(c.Activities))
	for _, a := range c.Activities {
		k := key{a.ScheduleID, a.Compensate}
		if seen[k] {
			return store.ErrInvalidCompletion
		}
		seen[k] = true
	}
	return nil
}

// CompleteWorkflowTask implements store.Store: one batch, one round trip.
func (s *Store) CompleteWorkflowTask(ctx context.Context, l store.Lease, c *store.Completion) ([]store.Lease, error) {
	if err := validateCompletion(c); err != nil {
		return nil, err
	}
	prevSeq, lastSeq := int64(0), int64(0)
	if n := len(c.History); n > 0 {
		if c.History[0].Seq < 1 {
			return nil, store.ErrInvalidCompletion
		}
		prevSeq, lastSeq = c.History[0].Seq-1, c.History[n-1].Seq
	}
	ending := c.Status != "" && !c.Status.Active()
	terminal := ending && c.Status != store.StatusContinued
	var claimed map[actKey]int64
	err := retry(ctx, func() error {
		if s.fast {
			var err error
			claimed, err = s.completeFast(ctx, l, c, ending, terminal, prevSeq, lastSeq)
			return err
		}
		b := &pgx.Batch{}
		b.Queue(fence, l.ID, l.Owner, l.Version, prevSeq, len(c.History) > 0)
		sh := completionShape{activityAt: -1}
		if terminal {
			b.Queue(noUndelivered, l.ID, nonNil(c.Consumed)) // NULL would disable the check
			sh.statements++
		}
		s.queueCompletion(b, l, c, ending, lastSeq, &sh)
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		var err error
		if claimed, err = readCompletion(br, sh); err != nil {
			return err
		}
		return br.Close()
	})
	if err != nil {
		err = mapErr(err)
		if sqlState(err) == "23505" { // a second task for one schedule id
			return nil, store.ErrInvalidCompletion
		}
		if errors.Is(err, store.ErrLeaseLost) || errors.Is(err, store.ErrInvalidCompletion) || errors.Is(err, store.ErrPendingEvents) {
			return nil, err
		}
		return nil, fmt.Errorf("complete workflow task: %w", err)
	}
	var leases []store.Lease
	for _, a := range c.Activities {
		if a.ClaimFor > 0 {
			leases = append(leases, store.Lease{ID: claimed[actKey{a.ScheduleID, a.Compensate}], Owner: l.Owner, Version: 1})
		}
	}
	return leases, nil
}

// completionShape tells readCompletion which statements were queued after
// the fence.
type completionShape struct {
	statements int // queued statements after the fence
	activityAt int // index of the activity insert among them, -1 if none
}

// queueCompletion queues the statements of c after the fence, counting
// them in sh.
func (s *Store) queueCompletion(b *pgx.Batch, l store.Lease, c *store.Completion, ending bool, lastSeq int64, sh *completionShape) {
	queue := func(sql string, args ...any) {
		b.Queue(sql, args...)
		sh.statements++
	}
	if len(c.History) > 0 {
		var a eventArgs
		for i := range c.History {
			a.add(&c.History[i], time.Time{})
		}
		queue(insertHistory, a.args(l.ID)...)
	}
	if len(c.Consumed) > 0 || len(c.CanceledTimers) > 0 {
		queue(deletePending, l.ID, nonNil(c.Consumed), nonNil(c.CanceledTimers))
	}
	if len(c.Timers) > 0 {
		var a eventArgs
		for i := range c.Timers {
			t := c.Timers[i]
			t.Type = history.TimerFired
			a.add(&t, t.At)
		}
		queue(insertPending, a.args(l.ID)...)
	}
	if len(c.Activities) > 0 {
		n := len(c.Activities)
		scheds, comps := make([]int32, n), make([]bool, n)
		names, queues := make([]string, n), make([]string, n)
		inputs, claims := make([][]byte, n), make([]int64, n)
		uncertain := make([]bool, n)
		for i, a := range c.Activities {
			scheds[i], comps[i], names[i], queues[i], inputs[i], claims[i] = a.ScheduleID, a.Compensate, a.Name, a.Queue, a.Input, micros(a.ClaimFor)
			uncertain[i] = a.Uncertain
		}
		sh.activityAt = sh.statements
		queue(insertActivities, l.ID, scheds, comps, names, queues, inputs, claims, l.Owner, uncertain)
	}
	for _, ch := range c.Children {
		queue(insertChild, ch.InstanceID, ch.Workflow, ch.Version, ch.Queue, l.ID, ch.ParentSched, ch.Input)
	}
	if len(c.ChildCancels) > 0 {
		queue(cancelChildren, l.ID, c.ChildCancels)
	}
	if len(c.CanceledActivities) > 0 {
		queue(cancelActivities, l.ID, c.CanceledActivities)
	}
	for _, k := range sortedLocks(c.Locks) {
		queue(acquireLock, l.ID, k.Key, k.ScheduleID, micros(k.TTL))
	}
	if !ending {
		queue(updateRunning, l.ID, lastSeq, string(c.Status), micros(c.KeepFor), micros(c.StickyFor), l.Owner, c.Err, c.ErrKinds)
		return
	}
	queue(endExecution, l.ID, lastSeq, string(c.Status), c.Output, c.Err, c.ErrKinds)
	if c.ContinueAsNew != nil {
		n := c.ContinueAsNew
		queue(continueAsNew, l.ID, n.Workflow, n.Version, n.Queue, n.Input)
		queue(carryPending, l.ID)
	}
	queue(dropEnded, l.ID)
	if c.Status != store.StatusContinued {
		ev := store.ChildOutcome(store.Execution{Status: c.Status, Output: c.Output, Err: c.Err, ErrKinds: c.ErrKinds})
		queue(notifyParent, l.ID, int16(ev.Type), ev.Payload, ev.Err, ev.ErrKinds)
	}
}

// actKey identifies an activity task of an execution.
type actKey struct {
	sched int32
	comp  bool
}

// readCompletion reads the batch results: the fence, then each queued
// statement. It returns the ids of the eagerly claimed activity tasks.
func readCompletion(br pgx.BatchResults, sh completionShape) (map[actKey]int64, error) {
	var seq int64
	if err := br.QueryRow().Scan(&seq); err != nil {
		return nil, err
	}
	var claimed map[actKey]int64
	for i := range sh.statements {
		if i != sh.activityAt {
			if _, err := br.Exec(); err != nil {
				return nil, err
			}
			continue
		}
		rows, err := br.Query()
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var k actKey
			var who string
			if err := rows.Scan(&id, &k.sched, &k.comp, &who, nil); err != nil { // nil: the notify column
				rows.Close()
				return nil, err
			}
			if who != "" {
				if claimed == nil {
					claimed = make(map[actKey]int64)
				}
				claimed[k] = id
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return claimed, nil
}

// claimActivities grants leases on up to $3 visible activity tasks of the
// queues $1 to owner $2 for $4 microseconds. visible_at moves to the lease
// expiry, so the ready index only holds claimable tasks.
const claimActivities = `
	WITH c AS (
		SELECT id, execution_id FROM flows.activity_task
		WHERE queue = ANY($1) AND visible_at <= now()
		ORDER BY visible_at, id
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	), k AS (
		-- An execution's locks live while its activities run (store.Completion.Locks);
		-- waits for a contender's lock row, as the workflow claim.
		UPDATE flows.lock k SET until = now() + k.ttl_us * interval '1 microsecond'
		FROM c WHERE k.execution_id = c.execution_id
	)
	UPDATE flows.activity_task t
	SET owner_id = $2, lease_version = t.lease_version + 1, attempt = t.attempt + 1,
	    locked_until = now() + $4 * interval '1 microsecond',
	    visible_at = now() + $4 * interval '1 microsecond'
	FROM c WHERE t.id = c.id
	RETURNING t.id, t.lease_version, t.execution_id, t.instance_id, t.schedule_id, t.name, t.queue,
	          t.input, t.compensate, t.attempt, t.cancel_requested, t.uncertain`

// ClaimActivityTasks implements store.Store.
func (s *Store) ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	var out []store.ActivityTask
	err := retry(ctx, func() error {
		out = out[:0]
		rows, err := s.pool.Query(ctx, claimActivities, queues, owner, limit, micros(ttl))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var t store.ActivityTask
			if err := rows.Scan(&t.Lease.ID, &t.Lease.Version, &t.ExecutionID, &t.InstanceID, &t.ScheduleID,
				&t.Name, &t.Queue, &t.Input, &t.Compensate, &t.Attempt, &t.CancelRequested, &t.Uncertain); err != nil {
				return err
			}
			t.Lease.Owner = owner
			out = append(out, t)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("claim activity tasks: %w", err)
	}
	return out, nil
}

// ExtendActivityTask implements store.Store.
func (s *Store) ExtendActivityTask(ctx context.Context, l store.Lease, ttl time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE flows.activity_task
		SET locked_until = now() + $4 * interval '1 microsecond', visible_at = now() + $4 * interval '1 microsecond'
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''`,
		l.ID, l.Owner, l.Version, micros(ttl))
	if err != nil {
		return fmt.Errorf("extend activity task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrLeaseLost
	}
	return nil
}

// RetryActivityTask implements store.Store.
func (s *Store) RetryActivityTask(ctx context.Context, l store.Lease, delay time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE flows.activity_task
		SET owner_id = '', locked_until = NULL, visible_at = now() + $4::bigint * interval '1 microsecond'
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
		RETURNING CASE WHEN $4::bigint <= 0 THEN `+notifyA+`queue) END`,
		l.ID, l.Owner, l.Version, micros(delay))
	if err != nil {
		return fmt.Errorf("retry activity task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return store.ErrLeaseLost
	}
	return nil
}

// Delivering an activity result wakes the execution (wakeForTask). The
// UPDATE always takes the row lock, so it waits for a holder that is
// releasing the lease, then sees the released row: a wake-up is never lost.
// While the execution is leased it leaves ready_at as it is (an unchanged
// indexed column keeps the update HOT, so it stays cheap): the holder
// recomputes ready_at when it releases the lease, in a statement that
// starts after it holds the row lock (after a fence), so its snapshot
// includes every result delivered before; while a lease is kept, ready_at
// stays at or before its expiry (claims, keeps and extensions maintain
// this), so an execution whose holder died is claimable once the lease
// expires.
//
// Lock order: wakeForTask locks the execution row before finishTask locks
// the task row, as a workflow task completion does (execution, then its
// tasks). finishTask's pending insert only takes a key share lock on the
// execution, which never conflicts with a completion's row lock, so the two
// never deadlock. finishTask raises 22012 when the lease is lost, rolling
// back the wake-up.
const (
	wakeForTask = `
		UPDATE flows.execution
		SET ready_at = CASE WHEN locked_until > now() THEN ready_at
		                    ELSE least(coalesce(ready_at, now()), now()) END
		WHERE id = (SELECT execution_id FROM flows.activity_task WHERE id = $1)
		  AND status IN ` + activeStatuses + `
		RETURNING CASE WHEN locked_until IS NULL OR locked_until <= now() THEN ` + notifyW + "queue)" + ` END`
	finishTask = `
		WITH t AS (
			DELETE FROM flows.activity_task
			WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
			RETURNING execution_id, schedule_id, attempt
		), e AS (
			SELECT x.id FROM flows.execution x, t WHERE x.id = t.execution_id AND x.status IN ` + activeStatuses + `
		), ins AS (
			INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, attempt, payload,
			                                 comp_data, err, err_kinds, uncertain, permanent)
			SELECT e.id, now(), $4, t.schedule_id, t.attempt, $5, $6, $7, $8, $9, $10 FROM e, t
		)
		SELECT CASE WHEN count(*) = 0 THEN (1 / (0 * random()))::bigint ELSE max(execution_id) END FROM t`
)

// CompleteActivityTask implements store.Store.
func (s *Store) CompleteActivityTask(ctx context.Context, l store.Lease, r store.ActivityResult) (*store.WorkflowTask, error) {
	ev := r.Event
	if !store.ValidResult(ev.Type) {
		return nil, store.ErrInvalidCompletion
	}
	var task *store.WorkflowTask
	err := retry(ctx, func() error {
		task = nil
		if s.fast {
			var err error
			task, err = s.completeActivityFast(ctx, l, &r)
			return err
		}
		b := &pgx.Batch{}
		b.Queue(wakeForTask, l.ID)
		b.Queue(finishTask, l.ID, l.Owner, l.Version, int16(ev.Type), ev.Payload, ev.CompData,
			ev.Err, ev.ErrKinds, ev.Uncertain, ev.Permanent)
		if r.Resume != nil {
			queueResume(b, *r.Resume, r.ResumeFor)
		}
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		if _, err := br.Exec(); err != nil {
			return err
		}
		var execID int64
		if err := br.QueryRow().Scan(&execID); err != nil {
			return err
		}
		if r.Resume != nil {
			t, ok, err := readResume(br, r.Resume.Owner)
			if err != nil {
				return err
			}
			if ok && t.Execution.ID == execID {
				task = &t
			}
		}
		return br.Close()
	})
	if err != nil {
		if err = mapErr(err); errors.Is(err, store.ErrLeaseLost) {
			return nil, err
		}
		return nil, fmt.Errorf("complete activity task: %w", err)
	}
	return task, nil
}

// deliver adds an event to the active execution of an instance id. The
// UPDATE waits for a completion holding the row and re-checks the status on
// the new row version, so an event is either delivered to an active
// execution (and seen by the fence of any later completion) or refused
// (GW-3). When the execution continued as new meanwhile, it retries on the
// next one.
const deliver = `
	WITH e AS (
		UPDATE flows.execution SET ready_at = least(coalesce(ready_at, now()), now())
		WHERE id = (SELECT id FROM flows.execution WHERE instance_id = $1 ORDER BY id DESC LIMIT 1)
		  AND status IN ` + activeStatuses + `
		RETURNING id, queue, status <> 'needs_attention' AND (locked_until IS NULL OR locked_until <= now()) AS free
	), ins AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, payload)
		SELECT id, now(), $2, $3, $4 FROM e
	)
	SELECT CASE WHEN free THEN ` + notifyW + "queue)" + ` END FROM e`

// Notifications (LISTEN/NOTIFY) are hints that work may be ready. The
// statements that make work ready for other owners send them on
// workChannel inside their transaction, so they are delivered at commit.
// The eager path sends none, because NOTIFY takes a global lock at commit.
// The payload is 'w' or 'a' followed by the queue name.
const workChannel = "flows_work"

// notifyW and notifyA start a pg_notify of a workflow or activity hint;
// the caller appends the queue expression and the closing parenthesis.
const (
	notifyW = "pg_notify('" + workChannel + "', 'w' || "
	notifyA = "pg_notify('" + workChannel + "', 'a' || "
)

// Signal implements store.Store.
func (s *Store) Signal(ctx context.Context, instanceID, name string, payload []byte) error {
	return s.deliver(ctx, instanceID, history.SignalReceived, name, payload)
}

// RequestCancel implements store.Store.
func (s *Store) RequestCancel(ctx context.Context, instanceID string) error {
	return s.deliver(ctx, instanceID, history.ExecutionCancelRequested, "", nil)
}

func (s *Store) deliver(ctx context.Context, instanceID string, typ history.Type, name string, payload []byte) error {
	for range maxRetries {
		var tag pgconn.CommandTag
		err := retry(ctx, func() error {
			var err error
			tag, err = s.pool.Exec(ctx, deliver, instanceID, int16(typ), name, payload)
			return err
		})
		if err != nil {
			return fmt.Errorf("deliver %v: %w", typ, err)
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		// The UPDATE re-checked the execution it picked before waiting on
		// its row lock: if that one continued as new meanwhile, the latest
		// is now its successor, and active. Try again.
		e, err := s.GetExecution(ctx, instanceID)
		if err != nil {
			return err
		}
		if !e.Status.Active() {
			return store.ErrNotActive
		}
	}
	return fmt.Errorf("deliver %v: instance kept continuing as new", typ)
}

// FastPath reports whether the hot paths run on pgconn (see fast.go).
func (s *Store) FastPath() bool { return s.fast }

// Pool returns the store's connection pool.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
