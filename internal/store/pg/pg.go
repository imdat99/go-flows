package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
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
	pool    *pgxpool.Pool
	fast    bool            // hot paths on pgconn (fast.go)
	hook    Hook            // nil: no end hook
	watches sync.Map        // done channel -> *ctxWatch (fast.go)
	listen  *pgx.ConnConfig // nil: Watch listens on a connection of the pool
}

// Hook runs inside the transaction that ends or parks an execution
// (store.Ends), after the store's own writes and under the same fence: what
// it writes commits atomically with the status, and an error (or a panic)
// rolls everything back: the completion fails, the task is retried later.
type Hook func(ctx context.Context, tx pgx.Tx, e store.EndInfo) error

// SetHook installs the end hook (before the store is used).
func (s *Store) SetHook(h Hook) { s.hook = h }

// inTx runs fn in a transaction, then the end hook with info, which fn
// completes with the identity of the execution it ended (read in its own
// batch, T10.9: no extra round trip), then commits.
func (s *Store) inTx(ctx context.Context, info *store.EndInfo, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := callHook(ctx, s.hook, tx, *info); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// endIdentity reads the identity of execution $1 for an end hook, queued
// last in the batch that ends it.
const endIdentity = `SELECT instance_id, workflow, version FROM flows.execution WHERE id = $1`

// hookError wraps an end hook's error (it is not retried as a transient one).
type hookError struct{ err error }

func (e *hookError) Error() string { return "end hook: " + e.err.Error() }
func (e *hookError) Unwrap() error { return e.err }

func callHook(ctx context.Context, h Hook, tx pgx.Tx, info store.EndInfo) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &hookError{fmt.Errorf("panic: %v", r)}
		}
	}()
	if err := h(ctx, tx, info); err != nil {
		return &hookError{err}
	}
	return nil
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
// The engine's statements mean to lock rows in one order (lockParent;
// lock rows in byte order of their keys). Cycles that order leaves (the
// one known: two executions ending at once, each the other's lock's
// waiter, lockWaitersOf), any it misses, and the application's own
// transactions are covered by retry.
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
	input, output, err, err_kinds, created_at, ended_at, trace`

// taskColumns are executionColumns without input and output: tasks do
// not carry them (store.WorkflowTask). A claim's row (taskColumns, three
// more, the pending arrays, then the claim count) stays within 33 columns:
// pgx reallocates a row's values whenever their capacity (the widest row so
// far, at least 32) exceeds a row's width by more than 32
// (pgproto3.DataRow.Decode), as the one-column due rows would then.
const taskColumns = `id, instance_id, workflow, version, queue, status, parent_id, parent_sched,
	err, err_kinds, created_at, ended_at, trace`

func scanExecution(row pgx.Row, extra ...any) (store.Execution, error) {
	return scanExecutionIO(row, true, extra)
}

// scanTask scans taskColumns, then extra.
func scanTask(row pgx.Row, extra ...any) (store.Execution, error) {
	return scanExecutionIO(row, false, extra)
}

func scanExecutionIO(row pgx.Row, io bool, extra []any) (store.Execution, error) {
	var e store.Execution
	var status string
	var ended pgtype.Timestamptz
	dest := []any{&e.ID, &e.InstanceID, &e.Workflow, &e.Version, &e.Queue, &status, &e.ParentID, &e.ParentSched}
	if io {
		dest = append(dest, &e.Input, &e.Output)
	}
	dest = append(dest, &e.Err, &e.ErrKinds, &e.CreatedAt, &ended, &e.Trace)
	dest = append(dest, extra...)
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
		INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at, trace)
		SELECT $1, $2, $3, $4, 'running', $5, $6, $7, now(), $8
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
		var e store.Execution
		var err error
		if s.fast {
			var created bool
			if e, created, err = s.createFast(ctx, &ne); err == nil && !created {
				err = pgx.ErrNoRows
			}
		} else {
			e, err = scanExecution(s.pool.QueryRow(ctx, insertExecution,
				ne.InstanceID, ne.Workflow, ne.Version, ne.Queue, ne.ParentID, ne.ParentSched, ne.Input, ne.Trace))
		}
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
// to owner $2 for $4 microseconds (GW-8: index scan in readiness order),
// and returns each with its visible pending events, in one statement (one
// implicit transaction: a claim never commits without its events read).
//
// Each queue is read on its own (LATERAL): with queue = ANY($1) the planner
// cannot read execution_ready_idx in order, and sorts the whole ready
// backlog on every claim (TestGW8_ClaimsReadOnlyWhatTheyClaim). Queues are
// deduplicated (a queue listed twice would fill the limit with one row
// twice: rows this statement locked are not skipped). With one queue, its
// first $3 rows are locked and claimed; with several, claimWorkflowQueues
// locks only the rows it claims. The pending events come from
// pending_event_execution_idx, one LATERAL per claimed execution, as
// arrays (readClaimed).
var claimWorkflow = `
	WITH c AS (
		SELECT x.id FROM (SELECT DISTINCT unnest($1::text[])) AS q(name)
		CROSS JOIN LATERAL (
			SELECT id, ready_at FROM flows.execution
			WHERE ` + claimableIn("q.name") + `
			ORDER BY ready_at, id
			LIMIT $3::bigint
			FOR NO KEY UPDATE SKIP LOCKED
		) x
		ORDER BY x.ready_at, x.id
		LIMIT $3::bigint
	)` + claimTail

// claimWorkflowQueues is claimWorkflow for several queues (T10.4): each
// locking up to $3 rows, it would hold rows (and make other statements
// skip them: acquireLock's previous holder) that the outer limit drops.
// The first $3 ready rows across the queues, read without locking, give
// each queue its share; each queue then locks its share (SKIP LOCKED: rows
// a concurrent claim holds are passed over for the queue's next ones).
// When that falls short (a queue had fewer unlocked rows than its share),
// the rest is topped up from every queue as claimWorkflow does: only then,
// and only up to the rest in each queue, does it lock rows it drops.
var claimWorkflowQueues = `
	WITH n AS (
		SELECT x.queue, count(*) AS n FROM (
			SELECT x.queue FROM (SELECT DISTINCT unnest($1::text[])) AS q(name)
			CROSS JOIN LATERAL (
				SELECT queue, ready_at, id FROM flows.execution
				WHERE ` + claimableIn("q.name") + `
				ORDER BY ready_at, id
				LIMIT $3::bigint
			) x
			ORDER BY x.ready_at, x.id
			LIMIT $3::bigint
		) x GROUP BY x.queue
	), s AS (
		SELECT x.id FROM n
		CROSS JOIN LATERAL (
			SELECT id FROM flows.execution
			WHERE ` + claimableIn("n.queue") + `
			ORDER BY ready_at, id
			LIMIT n.n
			FOR NO KEY UPDATE SKIP LOCKED
		) x
	), c AS (
		SELECT id FROM s
		UNION ALL (
			-- Short (rows of a share locked elsewhere, by an in-flight
			-- claim or completion): topped up from every queue, as
			-- claimWorkflow does, skipping the rows s locked (this
			-- transaction's: SKIP LOCKED would not).
			SELECT x.id FROM (SELECT DISTINCT unnest($1::text[])) AS q(name)
			CROSS JOIN LATERAL (
				SELECT id, ready_at FROM flows.execution
				WHERE ` + claimableIn("q.name") + ` AND id <> ALL(ARRAY(SELECT id FROM s))
				ORDER BY ready_at, id
				LIMIT $3::bigint - (SELECT count(*) FROM s)
				FOR NO KEY UPDATE SKIP LOCKED
			) x
			WHERE (SELECT count(*) FROM s) < $3::bigint
			ORDER BY x.ready_at, x.id
			LIMIT $3::bigint - (SELECT count(*) FROM s)
		)
	)` + claimTail

// claimableIn is the condition that an execution of queue q (an SQL
// expression) can be claimed by owner $2 now.
func claimableIn(q string) string {
	return `queue = ` + q + ` AND status IN ('running', 'compensating')
			  AND ready_at <= now()
			  AND (locked_until IS NULL OR locked_until < now())
			  AND (not_before IS NULL OR not_before <= now())
			  AND (sticky_until IS NULL OR sticky_until < now() OR sticky_owner = $2)`
}

// claimTail leases the executions of CTE c, extends their locks, and
// returns them with their pending events (claimWorkflow).
var claimTail = `
	, k AS (
		-- The locks of a running execution do not lapse (store.Completion.Locks).
		-- It waits for a lock row a contender holds (acquireLock never waits
		-- on an execution row: no cycle) and re-checks its holder.
		-- Lock rows are taken in key order (bytes, as sortedLocks): claims
		-- for the same execution (an execution and its activities) and
		-- ends do not deadlock.
		UPDATE flows.lock k SET until = now() + k.ttl_us * interval '1 microsecond'
		WHERE k.key IN (SELECT key FROM flows.lock WHERE execution_id = ANY(ARRAY(SELECT id FROM c)) AND NOT reserved
		                ORDER BY key COLLATE "C" FOR UPDATE)
	)
	, u AS (
		UPDATE flows.execution e
		SET owner_id = $2, lease_version = e.lease_version + 1,
		    locked_until = now() + $4::bigint * interval '1 microsecond', not_before = NULL,
		    -- Claims lost since the last completion (a release undoes its own, T11.3).
		    claims = e.claims + 1
		WHERE e.id = ANY(ARRAY(SELECT id FROM c)) -- a join may scan the table (generic plans)
		RETURNING e.id, e.instance_id, e.workflow, e.version, e.queue, e.status, e.parent_id, e.parent_sched,
		          e.err, e.err_kinds, e.created_at, e.ended_at, e.trace,
		          e.lease_version, e.last_seq, e.claims
	)
	SELECT u.id, u.instance_id, u.workflow, u.version, u.queue, u.status, u.parent_id, u.parent_sched,
	       u.err, u.err_kinds, u.created_at, u.ended_at, u.trace, u.lease_version, u.last_seq, now(), a.*, u.claims
	FROM u CROSS JOIN LATERAL ( -- an aggregate without GROUP BY: one row, even without events
		SELECT ` + pendingAggs + `
		FROM flows.pending_event p WHERE p.execution_id = u.id AND p.visible_at <= now()
	) a`

// claimStatement picks the claim of queues: claimWorkflow for one queue
// (listed once or more), claimWorkflowQueues for several.
func claimStatement(queues []string) (sql, name string) {
	for _, q := range queues {
		if q != queues[0] {
			return claimWorkflowQueues, nClaimWorkflowQueues
		}
	}
	return claimWorkflow, nClaimWorkflow
}

// pendingAggs aggregates the pending event columns of a group, in delivery
// order (visibility time, arrival), as arrays: id, then the event columns
// (eventColumns). Without events, each is an empty array.
var pendingAggs = func() string {
	cols := append([]string{"id"}, strings.Split(strings.Join(strings.Fields(eventColumns), ""), ",")...)
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = "coalesce(array_agg(p." + c + " ORDER BY p.visible_at, p.id), '{}')"
	}
	return strings.Join(out, ", ")
}()

const pendingColumnsP = `p.type, p.schedule_id, p.attempt, p.name, p.version, p.ref, p.queue, p.arg_hash,
	p.payload, p.comp_data, p.err, p.err_kinds, p.uncertain, p.permanent, p.at`

// ClaimWorkflowTasks implements store.Store.
func (s *Store) ClaimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, error) {
	tasks, _, err := s.claimWorkflowTasks(ctx, owner, queues, limit, ttl, false)
	return tasks, err
}

// ClaimWorkflowTasksDue implements store.DueClaimer: the claim and
// nextWorkflowDue in one round trip.
func (s *Store) ClaimWorkflowTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.WorkflowTask, time.Duration, error) {
	return s.claimWorkflowTasks(ctx, owner, queues, limit, ttl, true)
}

func (s *Store) claimWorkflowTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration, due bool) ([]store.WorkflowTask, time.Duration, error) {
	var tasks []store.WorkflowTask
	var next time.Duration
	err := retry(ctx, func() error {
		tasks, next = tasks[:0], 0
		if s.fast {
			var err error
			tasks, next, err = s.claimFast(ctx, owner, queues, limit, ttl, due)
			return err
		}
		sql, _ := claimStatement(queues)
		if !due {
			rows, err := s.pool.Query(ctx, sql, queues, owner, limit, micros(ttl))
			if err != nil {
				return err
			}
			tasks, err = readClaimed(rows, owner)
			return err
		}
		b := &pgx.Batch{}
		b.Queue(sql, queues, owner, limit, micros(ttl))
		b.Queue(nextWorkflowDue, queues)
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		rows, err := br.Query()
		if err != nil {
			return err
		}
		if tasks, err = readClaimed(rows, owner); err != nil {
			return err
		}
		if next, err = readDue(br); err != nil {
			return err
		}
		return br.Close()
	})
	if err != nil {
		return nil, 0, fmt.Errorf("claim workflow tasks: %w", err)
	}
	return tasks, next, nil
}

// readDue reads a next*Due result (microseconds; NULL: none).
func readDue(br pgx.BatchResults) (time.Duration, error) {
	var us *int64
	if err := br.QueryRow().Scan(&us); err != nil || us == nil {
		return 0, err
	}
	return time.Duration(*us) * time.Microsecond, nil
}

// nextWorkflowDue is, in microseconds from now, the earliest future time
// at which an active execution of the queues $1 becomes ready (ready_at: a
// timer, a retry, an event delivered for later) or is no longer held back
// (not_before: a hand-back); NULL when none. Index lookups per queue
// (execution_due_idx, execution_not_before_idx). Leases that expire are
// not covered: recovery waits for a timed poll (at most LeaseTTL/3 apart,
// T10.1).
const nextWorkflowDue = `
	SELECT (extract(epoch FROM min(least(r.t, n.t)) - now()) * 1000000)::bigint
	FROM unnest($1::text[]) AS q(name)
	LEFT JOIN LATERAL (
		-- Kept leases are left out (due_owner, T10.2): their ready_at is
		-- the lease expiry (recovery's, not a due time). So is a next due
		-- time falling while sticky to an owner that wakes for it (T10.3,
		-- store.Completion.DueKnown): it claims it; if it died, recovery's
		-- timed poll does. The index leaves them out, so the first entry
		-- after now() is the answer. Leases kept by a binary older than
		-- due_owner are filtered after the index (locked_until), over at
		-- most 64 rows: during a rolling upgrade, a timer behind more of
		-- them is left to the timed poll.
		SELECT min(x.ready_at) FROM (
			SELECT ready_at, locked_until FROM flows.execution
			WHERE queue = q.name AND status IN ('running', 'compensating') AND ready_at IS NOT NULL
			  AND (due_owner = '' OR due_owner <> sticky_owner) AND ready_at > now()
			ORDER BY ready_at LIMIT 64
		) AS x WHERE x.locked_until IS NULL
	) AS r(t) ON true
	LEFT JOIN LATERAL (
		SELECT not_before FROM flows.execution
		WHERE queue = q.name AND not_before IS NOT NULL AND status IN ('running', 'compensating')
		  AND not_before > now()
		ORDER BY not_before LIMIT 1
	) AS n(t) ON true`

// nextActivityDue is nextWorkflowDue for activity tasks: the earliest
// future visible_at of the unleased tasks of the queues $1 (retry delays;
// a running task's visible_at is its lease expiry, which is recovery's).
const nextActivityDue = `
	SELECT (extract(epoch FROM min(d.t) - now()) * 1000000)::bigint
	FROM unnest($1::text[]) AS q(name), LATERAL (
		SELECT visible_at FROM flows.activity_task
		WHERE queue = q.name AND locked_until IS NULL AND visible_at > now()
		ORDER BY visible_at LIMIT 1
	) AS d(t)`

// claimedEvents receives the pending event arrays of a claimed execution
// (pendingAggs: id, then eventColumns).
type claimedEvents struct {
	eventArgs
	ids []int64
	ats []pgtype.Timestamptz
}

// readClaimed reads claimWorkflow's rows: the execution, the lease version,
// last seq and store time, its pending events as arrays, then the claim
// count.
func readClaimed(rows pgx.Rows, owner string) ([]store.WorkflowTask, error) {
	defer rows.Close()
	var tasks []store.WorkflowTask
	var a claimedEvents
	for rows.Next() {
		var t store.WorkflowTask
		e, err := scanTask(rows, &t.Lease.Version, &t.LastSeq, &t.Now,
			&a.ids, &a.types, &a.scheds, &a.attempts, &a.names, &a.versions, &a.refs, &a.queues, &a.hashes,
			&a.payloads, &a.comps, &a.errs, &a.kinds, &a.uncertain, &a.permanent, &a.ats, &t.Claims)
		if err != nil {
			return nil, err
		}
		t.Execution = e
		t.Lease.ID, t.Lease.Owner = e.ID, owner
		if n := len(a.ids); n > 0 {
			t.Pending = make([]store.PendingEvent, n)
			for i := range n {
				t.Pending[i] = store.PendingEvent{ID: a.ids[i], Event: history.Event{
					Type: history.Type(a.types[i]), ScheduleID: a.scheds[i], Attempt: a.attempts[i],
					Name: a.names[i], Version: a.versions[i], Ref: a.refs[i], Queue: a.queues[i],
					Payload: a.payloads[i], CompData: a.comps[i], Err: a.errs[i], ErrKinds: a.kinds[i],
					Uncertain: a.uncertain[i], Permanent: a.permanent[i],
				}}
				finishEvent(&t.Pending[i].Event, a.ats[i], a.hashes[i])
			}
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func readTasks(rows pgx.Rows, owner string) ([]store.WorkflowTask, error) {
	defer rows.Close()
	var tasks []store.WorkflowTask
	for rows.Next() {
		var t store.WorkflowTask
		e, err := scanTask(rows, &t.Lease.Version, &t.LastSeq, &t.Now)
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
func readPending(rows pgx.Rows, tasks []store.WorkflowTask) error {
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
	SET owner_id = '', locked_until = NULL, sticky_owner = '', sticky_until = NULL, due_owner = '',
	    -- Undoes only its own claim: claims lost before it still count (T11.3).
	    claims = greatest(claims - 1, 0),
	    ready_at = (SELECT min(visible_at) FROM flows.pending_event WHERE execution_id = $1),
	    not_before = CASE WHEN $2::bigint > 0 THEN now() + $2::bigint * interval '1 microsecond' END
	WHERE id = $1
	RETURNING CASE WHEN ready_at IS NOT NULL THEN ` + notifyW + "queue)" + ` END -- held back: announced too (T9.1)`

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
		SELECT ` + taskColumns + `, lease_version, last_seq, now()
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
	rows, err := br.Query()
	if err != nil {
		return store.WorkflowTask{}, false, err
	}
	tasks, err := readTasks(rows, owner)
	if err != nil {
		return store.WorkflowTask{}, false, err
	}
	if rows, err = br.Query(); err != nil {
		return store.WorkflowTask{}, false, err
	}
	if err := readPending(rows, tasks); err != nil {
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

// lockParent locks the parent row of execution $1 (none for a root), first
// in a transaction ending $1 (T9.5). Transactions that wait for rows lock
// them in one order: execution rows, an ancestor's before a descendant's
// (a parent's completion: its row, then its children's in cancelChildren,
// compensateChild, cancelChildrenOf); then activity task rows; then lock
// rows (acquireLock, claims, dropLocks). An ending child locks its parent
// here, its own row in the fence, and notifies the parent (notifyParent)
// last. Exceptions that cannot close a cycle: a completion cancels its
// activities (task rows) and takes locks before compensateChild and
// cancelChildrenOf (no transaction holds a child's row while waiting for
// its parent's task or lock rows); claims, renewals and acquireLock's
// previous holder use SKIP LOCKED. parent_id never changes: reading it
// unlocked is safe.
const lockParent = `
	SELECT 1 FROM flows.execution
	WHERE id = (SELECT parent_id FROM flows.execution WHERE id = $1 AND parent_id <> 0)
	FOR NO KEY UPDATE`

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
// created leased to $8 (eager execution). $10 are their MaxAttempts.
const insertActivities = `
	INSERT INTO flows.activity_task (execution_id, instance_id, schedule_id, compensate, name, queue, input,
	                                 attempt, owner_id, lease_version, locked_until, visible_at, uncertain, trace,
	                                 max_attempts)
	SELECT $1, e.instance_id, u.sched, u.comp, u.name, u.queue, u.input,
	       CASE WHEN u.claim > 0 THEN 1 ELSE 0 END,
	       CASE WHEN u.claim > 0 THEN $8 ELSE '' END,
	       CASE WHEN u.claim > 0 THEN 1 ELSE 0 END,
	       CASE WHEN u.claim > 0 THEN now() + u.claim * interval '1 microsecond' END,
	       now() + greatest(u.claim, 0) * interval '1 microsecond', u.uncertain, e.trace, u.max_attempts
	FROM flows.execution e,
	     unnest($2::int[], $3::boolean[], $4::text[], $5::text[], $6::bytea[], $7::bigint[], $9::boolean[], $10::int[])
	       AS u(sched, comp, name, queue, input, claim, uncertain, max_attempts)
	WHERE e.id = $1
	RETURNING id, schedule_id, compensate, owner_id,
	          CASE WHEN owner_id = '' THEN ` + notifyA + "queue)" + ` END`

// acquireLock answers lock request $2 (schedule id $3, ttl $4 µs, hold $5
// µs, waiting $6 with its next attempt in $7 µs: store.LockRequest) of
// execution $1 with a pending LockAcquired or LockHeld; taking over an
// expired lock of another active execution delivers LockLost to it. The
// conflict clause re-checks the row a concurrent request may have created.
var acquireLock = `
	WITH cur AS (
		-- held_for: reserved for the waiter a release woke (T9.8), its turn
		-- not over: active, still waiting (its lock-wait timer pending).
		-- Read again, with the row, after waiting for a releasing
		-- transaction: the turn is on this row.
		SELECT k.execution_id, k.sched, k.until, k.ttl_us, k.hold_us, k.until > now() AS live, k.reserved,
		       k.reserved AND k.until > now() AND EXISTS (
		           SELECT 1 FROM flows.execution x
		           WHERE x.id = k.execution_id AND x.status IN ('running', 'compensating')
		             AND EXISTS (SELECT 1 FROM flows.pending_event p
		                         WHERE p.execution_id = x.id AND p.type = ` + typeTimerFired + `
		                           AND p.name = '` + store.LockWaitTimer("") + `' || $2)) AS held_for
		FROM flows.lock k WHERE k.key = $2 FOR UPDATE OF k
	), prev AS (
		-- The expired holder's row, unless it runs: its row is locked (being
		-- claimed or completed), its lease is live, or one of its activities
		-- runs. Then the lock stays its own (its renewals extend it). So it
		-- does, up to hold_us past until, while one of its activities waits
		-- (in its queue, for a retry, or for a new lease), or while events
		-- wait for its workflow task (T10.5: a result delivered, the task
		-- not claimed yet; not a future timer, nor a parked holder's).
		SELECT x.id, x.queue, x.locked_until, cur.sched FROM flows.execution x, cur
		WHERE x.id = cur.execution_id AND cur.execution_id <> $1 AND NOT cur.live AND NOT cur.reserved
		  AND (x.locked_until IS NULL OR x.locked_until <= now())
		  AND NOT EXISTS (SELECT 1 FROM flows.activity_task t
		                  WHERE t.execution_id = x.id
		                    AND (t.owner_id <> '' AND t.locked_until > now()
		                         OR cur.until + cur.hold_us * interval '1 microsecond' > now()))
		  AND NOT (cur.until + cur.hold_us * interval '1 microsecond' > now()
		           AND x.status IN ('running', 'compensating')
		           AND EXISTS (SELECT 1 FROM flows.pending_event p
		                       WHERE p.execution_id = x.id AND p.visible_at <= now()))
		FOR NO KEY UPDATE OF x SKIP LOCKED
	), avail AS (
		-- Free, expired and takeable, a reservation over, or $1's own.
		SELECT NOT EXISTS (SELECT 1 FROM cur WHERE execution_id <> $1 AND CASE WHEN reserved THEN held_for
		                                                                  ELSE live OR NOT EXISTS (SELECT 1 FROM prev) END) AS ok
	), take AS (
		INSERT INTO flows.lock AS k (key, execution_id, sched, until, ttl_us, hold_us)
		SELECT $2, $1, $3, now() + $4::bigint * interval '1 microsecond', $4, $5::bigint
		WHERE (SELECT ok FROM avail)
		ON CONFLICT (key) DO UPDATE
		SET execution_id = excluded.execution_id, sched = excluded.sched, until = excluded.until,
		    ttl_us = excluded.ttl_us, hold_us = excluded.hold_us, reserved = false
		WHERE k.until <= now() OR k.execution_id = $1 OR k.reserved
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
	), unqueue AS (
		-- Served, or no longer waiting (a request without $6).
		DELETE FROM flows.lock_waiter
		WHERE key = $2 AND execution_id = $1 AND (EXISTS (SELECT 1 FROM take) OR NOT $6::boolean)
	), enqueue AS (
		-- Refused and waiting: queued, or its turn refreshed in its place
		-- (to the back once it lapsed).
		INSERT INTO flows.lock_waiter AS w (key, execution_id, expires_at)
		SELECT $2, $1, now() + ($7::bigint + $4::bigint) * interval '1 microsecond'
		WHERE $6::boolean AND NOT EXISTS (SELECT 1 FROM take)
		ON CONFLICT (key, execution_id) DO UPDATE
		SET expires_at = excluded.expires_at, woken_until = NULL, -- a turn is a release's, once
		    id = CASE WHEN w.expires_at <= now() THEN nextval('flows.lock_waiter_id_seq') ELSE w.id END
	)
	INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, name, err)
	SELECT $1, now(),
	       CASE WHEN EXISTS (SELECT 1 FROM take) THEN ` + typeLockAcquired + ` ELSE ` + typeLockHeld + ` END, $3, $2,
	       CASE WHEN EXISTS (SELECT 1 FROM take) THEN ''
	            ELSE coalesce((SELECT e.instance_id FROM cur JOIN flows.execution e ON e.id = cur.execution_id),
	                          'another execution') END`

// waiterLive is the SQL condition that waiter row w has its turn: not
// lapsed, and still sleeping on its lock-wait timer (fired or not; a wait
// canceled or given up has none).
func waiterLive(w string) string {
	return w + `.expires_at > now() AND EXISTS (
		SELECT 1 FROM flows.pending_event p
		WHERE p.execution_id = ` + w + `.execution_id AND p.type = ` + typeTimerFired + `
		  AND p.name = '` + store.LockWaitTimer("") + `' || ` + w + `.key)`
}

// waitersOf selects, for each lock held by execution id (an SQL
// expression), the waiter whose turn it is: key, execution_id, ttl_us.
func waitersOf(id string) string {
	return `
		SELECT DISTINCT ON (w.key) w.key, w.execution_id, k.ttl_us FROM flows.lock_waiter w
		JOIN flows.lock k ON k.key = w.key AND k.execution_id = ` + id + ` AND NOT k.reserved
		JOIN flows.execution x ON x.id = w.execution_id AND x.status IN ('running', 'compensating') -- not parked
		WHERE ` + waiterLive("w") + ` AND w.execution_id <> ` + id + `
		ORDER BY w.key, w.id`
}

// lockWaitersOf and wakeWaitersOf wake, for each lock held by execution id
// (an SQL expression), its first live waiter (T9.8), before the locks are
// dropped. lockWaitersOf locks the waiters' execution rows first, waiting
// for a task of theirs in flight; wakeWaitersOf, a statement of its own
// (it sees what those tasks committed: a timer just scheduled), fires
// their lock-wait timers now, gives each its turn for the lock's TTL
// (woken_until), and makes them ready. The rows of other executions are
// locked after the ending one's task and children's rows (an exception to
// lockParent's order): an ending execution holds no lock rows then, so
// claims and acquireLock cannot close a cycle; two executions ending at
// once, each the other's lock's waiter, can (Postgres detects it, retry
// runs the batch again).
func lockWaitersOf(id string) string {
	return `
	SELECT x.id FROM flows.execution x
	WHERE x.id IN (SELECT execution_id FROM (` + waitersOf(id) + `) hd)
	ORDER BY x.id
	FOR NO KEY UPDATE`
}

func wakeWaitersOf(id string) string {
	return `
	WITH hd AS (` + waitersOf(id) + `
	), turn AS (
		-- Its turn: one TTL to come.
		UPDATE flows.lock_waiter w
		SET woken_until = now() + hd.ttl_us * interval '1 microsecond',
		    expires_at = greatest(w.expires_at, now() + hd.ttl_us * interval '1 microsecond')
		FROM hd WHERE w.key = hd.key AND w.execution_id = hd.execution_id
	), fire AS (
		UPDATE flows.pending_event p SET visible_at = now()
		FROM hd
		WHERE p.execution_id = hd.execution_id AND p.type = ` + typeTimerFired + `
		  AND p.name = '` + store.LockWaitTimer("") + `' || hd.key AND p.visible_at > now()
	), rdy AS (
		-- Rows lockWaitersOf locked are this transaction's (not skipped);
		-- a head that changed meanwhile is skipped when busy.
		UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
		WHERE x.id IN (SELECT id FROM flows.execution WHERE id IN (SELECT execution_id FROM hd)
		               FOR NO KEY UPDATE SKIP LOCKED)
		RETURNING x.queue, x.locked_until
	)
	SELECT ` + notifyW + "queue) FROM rdy WHERE locked_until IS NULL OR locked_until <= now()"
}

// dropLocksOf releases the locks of execution id (an SQL expression) and
// drops its waits: a lock whose waiter wakeWaitersOf just woke becomes that
// waiter's reservation, until its turn ends; the others are deleted (a
// reservation of id's own too: a woken waiter that leaves wakes nobody).
// The lock rows are locked in key order (bytes, sortedLocks) first
// (T10.4), as claims and acquireLock take them, not in the order the update and delete scans
// meet them.
func dropLocksOf(id string) string {
	return `
	WITH l AS MATERIALIZED (
		SELECT key FROM flows.lock WHERE execution_id = ` + id + ` ORDER BY key COLLATE "C" FOR UPDATE
	), w AS (DELETE FROM flows.lock_waiter WHERE execution_id = ` + id + `
	), r AS (
		-- The waiter wakeWaitersOf woke in this transaction (now() is the
		-- transaction's time): not one woken by an earlier release.
		UPDATE flows.lock k
		SET execution_id = w.execution_id, reserved = true, sched = 0, until = w.woken_until
		FROM flows.lock_waiter w
		WHERE k.key = ANY(ARRAY(SELECT key FROM l))
		  AND k.execution_id = ` + id + ` AND NOT k.reserved AND w.key = k.key AND w.execution_id <> ` + id + `
		  AND w.woken_until = now() + k.ttl_us * interval '1 microsecond'
		RETURNING k.key
	)
	DELETE FROM flows.lock
	WHERE key = ANY(ARRAY(SELECT key FROM l)) AND execution_id = ` + id + ` AND key NOT IN (SELECT key FROM r)`
}

var (
	lockWaiters = lockWaitersOf("$1")
	wakeWaiters = wakeWaitersOf("$1")
	dropLocks   = dropLocksOf("$1")
)

// compensateChild reopens the completed child of execution $1 at schedule
// id $2 for compensation, or answers $1 at once (store.Completion
// .CompensateChildren). Lock order: parent row (fenced), then child row,
// as cancelChildrenOf.
const compensateChild = `
	WITH ch AS (
		UPDATE flows.execution x
		SET status = 'compensating', ended_at = NULL, compensate_requested = true,
		    ready_at = least(coalesce(x.ready_at, now()), now())
		WHERE x.id = (SELECT id FROM flows.execution
		              WHERE parent_id = $1 AND parent_id <> 0 AND parent_sched = $2 AND status = 'completed'
		              ORDER BY id DESC LIMIT 1)
		RETURNING x.id, x.queue
	), req AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type)
		SELECT id, now(), ` + typeCompensationRequested + ` FROM ch
	), done AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id)
		SELECT $1, now(), ` + typeCompensationCompleted + `, $2 WHERE NOT EXISTS (SELECT 1 FROM ch)
	)
	SELECT ` + notifyW + "queue) FROM ch"

const (
	typeCompensationCompleted = "23" // history.CompensationCompleted
	typeCompensationRequested = "30" // history.ExecutionCompensationRequested
)

// sortedLocks orders lock requests by key: two executions taking the same
// locks in one checkpoint lock their rows in the same order (no deadlock).
// It is byte order, the order of every statement locking several lock rows
// (ORDER BY key COLLATE "C": the column's collation may differ, T10.4).
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
		INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at, trace)
		SELECT $1, $2, $3, $4, 'running', $5, $6, $7, now(), $8
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
	WHERE NOT EXISTS (SELECT 1 FROM ins)
	RETURNING schedule_id` // a row only when the id was taken (store.NewExecution.Taken)

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
// a compensation (the workflow's error), $9 due-known.
// The lease is kept only while the execution stays
// claimable (stickiness only applies when it is not kept). Released with work visible, it notifies other owners.
const updateRunning = `
	WITH v AS (
		SELECT coalesce(nullif($3::text, ''), status) AS status,
		       $4::bigint > 0 AND coalesce(nullif($3::text, ''), status) IN ('running', 'compensating') AS keep,
		       (SELECT min(visible_at) FROM flows.pending_event WHERE execution_id = $1) AS next_due
		FROM flows.execution WHERE id = $1
	)
	UPDATE flows.execution e
	SET last_seq = CASE WHEN $2::bigint > 0 THEN $2::bigint ELSE e.last_seq END,
	    status = v.status,
	    claims = 0, -- completed: the claims start over (T11.3)
	    err = CASE WHEN $3::text IN ('needs_attention', 'compensating') THEN $7::text ELSE e.err END,
	    err_kinds = CASE WHEN $3::text IN ('needs_attention', 'compensating') THEN $8::text ELSE e.err_kinds END,
	    -- A kept lease is claimable at its expiry: events delivered under it
	    -- (wakeForTask) leave ready_at alone, so a dead holder's execution
	    -- is found then, possibly with nothing pending (an empty task).
	    ready_at = CASE WHEN v.keep
	        THEN least(v.next_due, now() + $4 * interval '1 microsecond')
	        ELSE v.next_due END,
	    owner_id = CASE WHEN v.keep THEN e.owner_id ELSE '' END,
	    -- The next due time is the owner's (due_owner, T10.2, T10.3): a
	    -- kept lease's, or the first event falling due while the execution
	    -- is sticky to it and within $9 (store.Completion.DueKnown: the owner
	    -- wakes for it). The mark counts while sticky_owner names the owner:
	    -- a kept lease's sticky_owner is its owner (no sticky_until).
	    due_owner = CASE WHEN v.keep OR coalesce($5::bigint > 0 AND $9::bigint > 0 AND
	        v.next_due <= now() + least($5::bigint, $9::bigint) * interval '1 microsecond', false)
	        THEN $6 ELSE '' END,
	    locked_until = CASE WHEN v.keep THEN now() + $4 * interval '1 microsecond' END,
	    sticky_owner = CASE WHEN v.keep OR $5::bigint > 0 THEN $6 ELSE '' END,
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
			INSERT INTO flows.execution (instance_id, workflow, version, queue, status, parent_id, parent_sched, input, ready_at, trace)
			SELECT old.instance_id, $2, $3, $4, 'running', old.parent_id, old.parent_sched, $5, now(), $6 FROM old
			RETURNING id
		)
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, version, queue, payload)
		SELECT id, now(), ` + typeStarted + `, $2, $3, $4, $5 FROM ins
		RETURNING ` + notifyW + "$4)"
	// carrySignals gives the new execution the events the old one received
	// but had not acted on ($2 types, $3 names, $4 payloads, in order),
	// before carryPending's (store.Completion.Carry).
	carrySignals = `
		INSERT INTO flows.pending_event (execution_id, visible_at, type, name, payload)
		SELECT n.id, now(), u.type, u.name, u.payload
		FROM unnest($2::smallint[], $3::text[], $4::bytea[]) WITH ORDINALITY AS u(type, name, payload, ord),
		     (SELECT id FROM flows.execution
		      WHERE instance_id = (SELECT instance_id FROM flows.execution WHERE id = $1)
		        AND status = 'running' ORDER BY id DESC LIMIT 1) n
		ORDER BY u.ord`
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
	// end at their next heartbeat (ErrLeaseLost) instead of running on.
	// dropLocks (dropLocksOf), a statement of its own after it, releases its
	// business locks: task rows before lock rows, as activity claims lock
	// them (see lockParent for the whole order).
	dropEnded = `
		WITH p AS (DELETE FROM flows.pending_event WHERE execution_id = $1)
		DELETE FROM flows.activity_task WHERE execution_id = $1`

	// cancelChildrenOf asks the active children of an ended execution $1 to
	// cancel (cancel by tree).
	cancelChildrenOf = `
		WITH ch AS (
			UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
			WHERE x.parent_id = $1 AND x.parent_id <> 0 AND x.status IN ` + activeStatuses + `
			RETURNING x.id, x.queue, x.status <> 'needs_attention' AND (x.locked_until IS NULL OR x.locked_until <= now()) AS free
		), ins AS (
			INSERT INTO flows.pending_event (execution_id, visible_at, type)
			SELECT id, now(), ` + typeCancelRequested + ` FROM ch
		)
		SELECT ` + notifyW + "queue) FROM ch WHERE free"
	// notifyParent delivers the child's outcome ($2..$7) to its active
	// parent.
	notifyParent = `
		WITH c AS (
			SELECT parent_id, parent_sched, instance_id, compensate_requested FROM flows.execution
			WHERE id = $1 AND parent_id <> 0
		), p AS (
			UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
			FROM c WHERE x.id = c.parent_id AND x.status IN ` + activeStatuses + `
			RETURNING x.id, x.queue
		), ins AS (
			-- A child reopened by its parent's compensation answers that.
			INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, ref, payload, err, err_kinds)
			SELECT p.id, now(),
			       CASE WHEN c.compensate_requested THEN ` + typeCompensationCompleted + ` ELSE $2 END,
			       c.parent_sched, c.instance_id,
			       CASE WHEN c.compensate_requested THEN NULL ELSE $3::bytea END,
			       CASE WHEN c.compensate_requested THEN '' ELSE $4 END,
			       CASE WHEN c.compensate_requested THEN '' ELSE $5 END
			FROM p, c
		)
		SELECT ` + notifyW + "queue) FROM p"
)

func validateCompletion(c *store.Completion) error {
	if !store.ValidShape(c) {
		return store.ErrInvalidCompletion
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
	c.Resumed = false
	hooked := s.hook != nil && store.Ends(c.Status)
	var claimed map[actKey]int64
	var fastLeases []store.Lease
	fast := s.fast && !hooked
	err := retry(ctx, func() error {
		if fast {
			var err error
			fastLeases, err = s.completeFast(ctx, l, c, ending, terminal, prevSeq, lastSeq)
			return err
		}
		var err error
		claimed, err = s.completeWithPgx(ctx, l, c, hooked, ending, terminal, prevSeq, lastSeq)
		return err
	})
	if err != nil {
		if he := (*hookError)(nil); errors.As(err, &he) {
			return nil, fmt.Errorf("complete workflow task: %w", err) // the hook's own codes mean nothing here
		}
		err = mapErr(err)
		if sqlState(err) == "23505" { // a second task for one schedule id
			return nil, store.ErrInvalidCompletion
		}
		if errors.Is(err, store.ErrLeaseLost) || errors.Is(err, store.ErrInvalidCompletion) || errors.Is(err, store.ErrPendingEvents) {
			return nil, err
		}
		return nil, fmt.Errorf("complete workflow task: %w", err)
	}
	if fast {
		return fastLeases, nil
	}
	return eagerLeases(c, l.Owner, claimed), nil
}

// completeWithPgx is CompleteWorkflowTask on the pgx path: one batch (in
// a transaction of its own when hooked).
func (s *Store) completeWithPgx(ctx context.Context, l store.Lease, c *store.Completion, hooked, ending, terminal bool,
	prevSeq, lastSeq int64,
) (map[actKey]int64, error) {
	c.Resumed = false // set again below on success (retry may run this twice)
	b := &pgx.Batch{}
	sh := completionShape{activityAt: -1, parentFirst: terminal}
	if terminal {
		b.Queue(lockParent, l.ID)
	}
	b.Queue(fence, l.ID, l.Owner, l.Version, prevSeq, len(c.History) > 0)
	if terminal {
		b.Queue(noUndelivered, l.ID, nonNil(c.Consumed)) // NULL would disable the check
		sh.statements++
	}
	s.queueCompletion(b, l, c, ending, lastSeq, &sh)
	if hooked {
		b.Queue(endIdentity, l.ID) // for the hook, in the same round trip (before the resume's statements)
	}
	resume := resumes(c, ending)
	if resume {
		queueResume(b, l, c.KeepFor) // T9.7: in the same round trip
	}
	var claimed map[actKey]int64
	send := func(sb interface {
		SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	}, info *store.EndInfo,
	) error {
		br := sb.SendBatch(ctx, b)
		defer br.Close()
		var err error
		if claimed, err = readCompletion(br, sh, c.Children); err != nil {
			return err
		}
		if info != nil {
			info.ExecutionID = l.ID
			if err := br.QueryRow().Scan(&info.InstanceID, &info.Workflow, &info.Version); err != nil {
				return err
			}
		}
		if resume {
			t, ok, err := readResume(br, l.Owner)
			if err != nil {
				return err
			}
			if ok {
				*c.Resume, c.Resumed = t, true
			}
		}
		return br.Close()
	}
	if hooked {
		info := store.EndInfo{Status: c.Status, Err: c.Err, ErrKinds: c.ErrKinds}
		err := s.inTx(ctx, &info, func(tx pgx.Tx) error { return send(tx, &info) })
		return claimed, err
	}
	return claimed, send(s.pool, nil)
}

// eagerLeases lists the leases of the eagerly claimed activity tasks, in the
// order of c.Activities.
func eagerLeases(c *store.Completion, owner string, claimed map[actKey]int64) []store.Lease {
	var leases []store.Lease
	for _, a := range c.Activities {
		if a.ClaimFor > 0 {
			leases = append(leases, store.Lease{ID: claimed[actKey{a.ScheduleID, a.Compensate}], Owner: owner, Version: 1})
		}
	}
	return leases
}

// resumes reports whether completion c returns the next task under its
// kept lease (store.Completion.Resume).
func resumes(c *store.Completion, ending bool) bool {
	return c.Resume != nil && c.KeepFor > 0 && !ending
}

// completionShape tells readCompletion which statements were queued after
// the fence.
type completionShape struct {
	parentFirst bool // lockParent queued before the fence
	statements  int  // queued statements after the fence
	activityAt  int  // index of the activity insert among them, -1 if none
	childAt     int  // index of the first child insert, then one per child
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
		uncertain, maxAttempts := make([]bool, n), make([]int32, n)
		for i, a := range c.Activities {
			scheds[i], comps[i], names[i], queues[i], inputs[i], claims[i] = a.ScheduleID, a.Compensate, a.Name, a.Queue, a.Input, micros(a.ClaimFor)
			uncertain[i], maxAttempts[i] = a.Uncertain, a.MaxAttempts
		}
		sh.activityAt = sh.statements
		queue(insertActivities, l.ID, scheds, comps, names, queues, inputs, claims, l.Owner, uncertain, maxAttempts)
	}
	sh.childAt = sh.statements
	for _, ch := range c.Children {
		queue(insertChild, ch.InstanceID, ch.Workflow, ch.Version, ch.Queue, l.ID, ch.ParentSched, ch.Input, ch.Trace)
	}
	if len(c.ChildCancels) > 0 {
		queue(cancelChildren, l.ID, c.ChildCancels)
	}
	if len(c.CanceledActivities) > 0 {
		queue(cancelActivities, l.ID, c.CanceledActivities)
	}
	for _, k := range sortedLocks(c.Locks) {
		queue(acquireLock, l.ID, k.Key, k.ScheduleID, micros(k.TTL), micros(k.Hold), k.Wait, micros(k.WaitRetry))
	}
	for _, sched := range c.CompensateChildren {
		queue(compensateChild, l.ID, sched)
	}
	if !ending {
		queue(updateRunning, l.ID, lastSeq, string(c.Status), micros(c.KeepFor), micros(c.StickyFor), l.Owner, c.Err, c.ErrKinds,
			micros(c.DueKnown))
		return
	}
	queue(endExecution, l.ID, lastSeq, string(c.Status), c.Output, c.Err, c.ErrKinds)
	if c.ContinueAsNew != nil {
		n := c.ContinueAsNew
		queue(continueAsNew, l.ID, n.Workflow, n.Version, n.Queue, n.Input, n.Trace)
		if len(c.Carry) > 0 {
			types := make([]int16, len(c.Carry))
			names, payloads := make([]string, len(c.Carry)), make([][]byte, len(c.Carry))
			for i, ev := range c.Carry {
				types[i], names[i], payloads[i] = int16(ev.Type), ev.Name, ev.Payload
			}
			queue(carrySignals, l.ID, types, names, payloads)
		}
		queue(carryPending, l.ID)
	}
	// Children's rows, then task rows, then lock rows (lockParent).
	if c.Status != store.StatusContinued {
		queue(cancelChildrenOf, l.ID)
	}
	queue(dropEnded, l.ID)
	queue(lockWaiters, l.ID)
	queue(wakeWaiters, l.ID)
	queue(dropLocks, l.ID)
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
// A child insert returns a row when its id was taken: children[k].Taken.
func readCompletion(br pgx.BatchResults, sh completionShape, children []store.NewExecution) (map[actKey]int64, error) {
	if sh.parentFirst {
		if _, err := br.Exec(); err != nil {
			return nil, err
		}
	}
	var seq int64
	if err := br.QueryRow().Scan(&seq); err != nil {
		return nil, err
	}
	var claimed map[actKey]int64
	for i := range sh.statements {
		if k := i - sh.childAt; k >= 0 && k < len(children) {
			rows, err := br.Query()
			if err != nil {
				return nil, err
			}
			children[k].Taken = rows.Next()
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, err
			}
			continue
		}
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
// Each queue (deduplicated) is read on its own, as in claimWorkflow.
const claimActivities = `
	WITH c AS (
		SELECT x.id, x.execution_id, x.exhausted FROM (SELECT DISTINCT unnest($1::text[])) AS q(name)
		CROSS JOIN LATERAL (
			-- The row as locked (FOR UPDATE re-reads a row updated meanwhile).
			-- A lease that expired without a result was a lost attempt: the
			-- one beyond max_attempts after it is not run (T11.3).
			SELECT id, execution_id, visible_at,
			       owner_id <> '' AND max_attempts > 0 AND attempt >= max_attempts AS exhausted
			FROM flows.activity_task
			WHERE queue = q.name AND visible_at <= now()
			ORDER BY visible_at, id
			LIMIT $3::bigint
			FOR UPDATE SKIP LOCKED
		) x
		ORDER BY x.visible_at, x.id
		LIMIT $3::bigint
	), k AS (
		-- An execution's locks live while its activities run (store.Completion.Locks);
		-- waits for a contender's lock row, as the workflow claim.
		UPDATE flows.lock k SET until = now() + k.ttl_us * interval '1 microsecond'
		WHERE k.key IN (SELECT key FROM flows.lock WHERE execution_id = ANY(ARRAY(SELECT execution_id FROM c)) AND NOT reserved
		                ORDER BY key COLLATE "C" FOR UPDATE)
	)
	UPDATE flows.activity_task t
	SET owner_id = $2, lease_version = t.lease_version + 1, attempt = t.attempt + 1,
	    -- A lease that expired without a result: its attempt may have run.
	    uncertain = t.uncertain OR (t.owner_id <> '' AND NOT t.compensate),
	    locked_until = now() + $4::bigint * interval '1 microsecond',
	    visible_at = now() + $4::bigint * interval '1 microsecond'
	WHERE t.id = ANY(ARRAY(SELECT id FROM c)) -- a join may scan the table (generic plans)
	RETURNING t.id, t.lease_version, t.execution_id, t.instance_id, t.schedule_id, t.name, t.queue,
	          t.input, t.compensate, t.attempt, t.cancel_requested, t.uncertain, t.comp_data, t.trace,
	          (SELECT c.exhausted FROM c WHERE c.id = t.id)`

// ClaimActivityTasks implements store.Store.
func (s *Store) ClaimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, error) {
	out, _, err := s.claimActivityTasks(ctx, owner, queues, limit, ttl, false)
	return out, err
}

// ClaimActivityTasksDue implements store.DueClaimer.
func (s *Store) ClaimActivityTasksDue(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration) ([]store.ActivityTask, time.Duration, error) {
	return s.claimActivityTasks(ctx, owner, queues, limit, ttl, true)
}

func (s *Store) claimActivityTasks(ctx context.Context, owner string, queues []string, limit int, ttl time.Duration, due bool) ([]store.ActivityTask, time.Duration, error) {
	var out []store.ActivityTask
	var next time.Duration
	err := retry(ctx, func() error {
		out, next = out[:0], 0
		if s.fast {
			var err error
			out, next, err = s.claimActivitiesFast(ctx, owner, queues, limit, ttl, due)
			return err
		}
		// The claim and, when asked, nextActivityDue in one batch (one
		// implicit transaction, one round trip), as the fast path does.
		b := &pgx.Batch{}
		b.Queue(claimActivities, queues, owner, limit, micros(ttl))
		if due {
			b.Queue(nextActivityDue, queues)
		}
		br := s.pool.SendBatch(ctx, b)
		defer br.Close()
		rows, err := br.Query()
		if err != nil {
			return err
		}
		for rows.Next() {
			var t store.ActivityTask
			if err := rows.Scan(&t.Lease.ID, &t.Lease.Version, &t.ExecutionID, &t.InstanceID, &t.ScheduleID,
				&t.Name, &t.Queue, &t.Input, &t.Compensate, &t.Attempt, &t.CancelRequested, &t.Uncertain, &t.CompData, &t.Trace,
				&t.Exhausted); err != nil {
				rows.Close()
				return err
			}
			t.Lease.Owner = owner
			out = append(out, t)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if due {
			if next, err = readDue(br); err != nil {
				return err
			}
		}
		return br.Close()
	})
	if err != nil {
		return nil, 0, fmt.Errorf("claim activity tasks: %w", err)
	}
	return out, next, nil
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
func (s *Store) RetryActivityTask(ctx context.Context, l store.Lease, delay time.Duration, uncertain bool, compData []byte) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE flows.activity_task
		SET owner_id = '', locked_until = NULL, visible_at = now() + $4::bigint * interval '1 microsecond',
		    uncertain = uncertain OR ($5 AND NOT compensate),
		    comp_data = CASE WHEN $6::bytea IS NULL OR compensate THEN comp_data ELSE $6::bytea END
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3 AND owner_id <> ''
		RETURNING `+notifyA+`queue) -- a retry delay is announced too (T9.1)`,
		l.ID, l.Owner, l.Version, micros(delay), uncertain, compData)
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
// Work made ready later for other engines is announced too: a hand-back, a
// retry delay, and the executions an engine stops holding (Unstick, at
// Stop: their timers). The claim an idle engine makes on the hint tells it
// when the work falls due (store.DueClaimer, T9.1). A timer committed by a
// running engine is not: the execution is sticky to it, which knows the
// due time (its worker's own due times).
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
