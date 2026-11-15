package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/imdat99/go-flows/internal/store"
)

// renewWorkflows and renewActivities extend the leases owner $1 still holds
// ($2 ids, $3 versions) by $4 microseconds, skipping rows locked elsewhere
// (ported from saga RenewLeases). They return each held lease with whether
// it was renewed; a lease missing from the result was lost.
const (
	renewWorkflows = `
		WITH held AS (
			SELECT e.id FROM flows.execution e
			JOIN unnest($2::bigint[], $3::bigint[]) AS l(id, version) ON e.id = l.id AND e.lease_version = l.version
			WHERE e.owner_id = $1 AND e.status IN ('running', 'compensating')
		), locked AS (
			SELECT e.id FROM flows.execution e
			JOIN unnest($2::bigint[], $3::bigint[]) AS l(id, version) ON e.id = l.id AND e.lease_version = l.version
			WHERE e.owner_id = $1 AND e.status IN ('running', 'compensating')
			FOR NO KEY UPDATE OF e SKIP LOCKED
		), renewed AS (
			UPDATE flows.execution e SET locked_until = now() + $4 * interval '1 microsecond'
			FROM locked WHERE e.id = locked.id
			RETURNING e.id
		)
		SELECT id, true, false FROM renewed
		UNION ALL
		SELECT id, false, false FROM held WHERE id NOT IN (SELECT id FROM renewed)`
	renewActivities = `
		WITH held AS (
			SELECT t.id FROM flows.activity_task t
			JOIN unnest($2::bigint[], $3::bigint[]) AS l(id, version) ON t.id = l.id AND t.lease_version = l.version
			WHERE t.owner_id = $1
		), locked AS (
			SELECT t.id FROM flows.activity_task t
			JOIN unnest($2::bigint[], $3::bigint[]) AS l(id, version) ON t.id = l.id AND t.lease_version = l.version
			WHERE t.owner_id = $1
			FOR UPDATE OF t SKIP LOCKED
		), renewed AS (
			UPDATE flows.activity_task t
			SET locked_until = now() + $4 * interval '1 microsecond', visible_at = now() + $4 * interval '1 microsecond'
			FROM locked WHERE t.id = locked.id
			RETURNING t.id, t.cancel_requested
		)
		SELECT id, true, cancel_requested FROM renewed
		UNION ALL
		SELECT id, false, false FROM held WHERE id NOT IN (SELECT id FROM renewed)`
)

// RenewLeases implements store.Store: both kinds in one batch (one round
// trip, one implicit transaction; both statements use SKIP LOCKED, so the
// row locks of the first never make the second wait).
//
// A lease that moves on between the statement's snapshot and its row lock
// is reported busy rather than lost; the next renewal reports it lost.
// Leases of another owner than owner are reported lost.
func (s *Store) RenewLeases(ctx context.Context, owner string, workflows, activities []store.Lease, ttl time.Duration) (store.Renewal, error) {
	var r store.Renewal
	workflows, r.LostWorkflows = ownedBy(workflows, owner)
	activities, r.LostActivities = ownedBy(activities, owner)
	if len(workflows) == 0 && len(activities) == 0 {
		return r, nil
	}
	b := &pgx.Batch{}
	wIDs, wVers := leaseArrays(workflows)
	aIDs, aVers := leaseArrays(activities)
	b.Queue(renewWorkflows, owner, wIDs, wVers, micros(ttl))
	b.Queue(renewActivities, owner, aIDs, aVers, micros(ttl))
	br := s.pool.SendBatch(ctx, b)
	defer br.Close()
	lost, busy, _, err := readRenewal(br, wIDs)
	if err != nil {
		return r, fmt.Errorf("renew leases: %w", err)
	}
	r.LostWorkflows, r.BusyWorkflows = append(r.LostWorkflows, lost...), busy
	var canceled []int64
	if lost, busy, canceled, err = readRenewal(br, aIDs); err != nil {
		return r, fmt.Errorf("renew leases: %w", err)
	}
	r.LostActivities, r.BusyActivities, r.CanceledActivities = append(r.LostActivities, lost...), busy, canceled
	if err := br.Close(); err != nil {
		return r, fmt.Errorf("renew leases: %w", err)
	}
	return r, nil
}

// ownedBy splits ls into the leases of owner and the ids of the others.
func ownedBy(ls []store.Lease, owner string) (own []store.Lease, others []int64) {
	for _, l := range ls {
		if l.Owner == owner {
			own = append(own, l)
		} else {
			others = append(others, l.ID)
		}
	}
	return own, others
}

func leaseArrays(ls []store.Lease) (ids, versions []int64) {
	ids, versions = make([]int64, len(ls)), make([]int64, len(ls))
	for i, l := range ls {
		ids[i], versions[i] = l.ID, l.Version
	}
	return ids, versions
}

func readRenewal(br pgx.BatchResults, ids []int64) (lost, busy, canceled []int64, err error) {
	rows, err := br.Query()
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	seen := make(map[int64]bool, len(ids))
	for rows.Next() {
		var id int64
		var renewed, cancel bool
		if err := rows.Scan(&id, &renewed, &cancel); err != nil {
			return nil, nil, nil, err
		}
		seen[id] = true
		if !renewed {
			busy = append(busy, id)
		}
		if cancel {
			canceled = append(canceled, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	for _, id := range ids {
		if !seen[id] {
			lost = append(lost, id)
		}
	}
	return lost, busy, canceled, nil
}

// PurgeEnded implements store.Store. The foreign keys cascade to history,
// pending events and activity tasks (GW-4). limit bounds executions, not
// history rows: keep it modest when histories are long.
func (s *Store) PurgeEnded(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM flows.execution WHERE id IN (
			SELECT id FROM flows.execution
			WHERE ended_at < now() - $1 * interval '1 microsecond'
			ORDER BY ended_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED)`, micros(olderThan), limit)
	if err != nil {
		return 0, fmt.Errorf("purge ended executions: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// terminate ends the latest execution of $1 if active (raising 22012
// otherwise, which aborts the batch); dropTerminated and notifyTerminated
// then clean up and notify its parent. The latest execution is the one just
// terminated: no new execution of an instance id can start while one exists.
const (
	terminate = `
		WITH e AS (
			SELECT id, last_seq FROM flows.execution
			WHERE id = (SELECT id FROM flows.execution WHERE instance_id = $1 ORDER BY id DESC LIMIT 1)
			  AND status IN ` + activeStatuses + `
			FOR NO KEY UPDATE
		), h AS (
			INSERT INTO flows.history (execution_id, seq, type, err)
			SELECT id, last_seq + 1, ` + typeTerminated + `, $2 FROM e
		), u AS (
			UPDATE flows.execution x
			SET status = 'terminated', err = $2, err_kinds = '', ended_at = now(), last_seq = e.last_seq + 1,
			    ready_at = NULL, owner_id = '', locked_until = NULL, lease_version = x.lease_version + 1,
			    sticky_owner = '', sticky_until = NULL
			FROM e WHERE x.id = e.id
			RETURNING x.id
		)
		SELECT CASE WHEN count(*) = 0 THEN (1 / (0 * random()))::bigint ELSE max(id) END FROM u`
	latestOf = `(SELECT id FROM flows.execution WHERE instance_id = $1 ORDER BY id DESC LIMIT 1)`
)

const typeTerminated = "6" // history.ExecutionTerminated

const typeRetryCompensation = "25" // history.CompensationRetryRequested

// reopenCompensation makes the latest execution of $1, parked because a
// compensation failed, compensating again with a retry request.
const reopenCompensation = `
	WITH e AS (
		UPDATE flows.execution SET status = 'compensating', ready_at = least(coalesce(ready_at, now()), now())
		WHERE id = ` + latestOf + `
		  AND status = 'needs_attention' AND '` + store.KindCompensationFailed + `' = ANY(string_to_array(err_kinds, ','))
		RETURNING id, queue
	), ins AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type)
		SELECT id, now(), ` + typeRetryCompensation + ` FROM e
	)
	SELECT ` + notifyW + "queue)" + ` FROM e`

// ReopenCompensation implements store.Store.
func (s *Store) ReopenCompensation(ctx context.Context, instanceID string) error {
	var tag pgconn.CommandTag
	err := retry(ctx, func() error {
		var err error
		tag, err = s.pool.Exec(ctx, reopenCompensation, instanceID)
		return err
	})
	if err != nil {
		return fmt.Errorf("reopen compensation: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	if _, err := s.GetExecution(ctx, instanceID); err != nil {
		return err
	}
	return store.ErrNotActive
}

// notifyTerminated delivers ChildFailed (terminated) to the active parent
// of the latest execution of $1.
const notifyTerminated = `
	WITH c AS (
		SELECT parent_id, parent_sched, instance_id FROM flows.execution
		WHERE id = ` + latestOf + ` AND parent_id <> 0
	), p AS (
		UPDATE flows.execution x SET ready_at = least(coalesce(x.ready_at, now()), now())
		FROM c WHERE x.id = c.parent_id AND x.status IN ` + activeStatuses + `
		RETURNING x.id, x.queue
	), ins AS (
		INSERT INTO flows.pending_event (execution_id, visible_at, type, schedule_id, ref, err, err_kinds)
		SELECT p.id, now(), ` + typeChildFailed + `, c.parent_sched, c.instance_id, $2, $3 FROM p, c
	)
	SELECT ` + notifyW + "queue) FROM p"

// Terminate implements store.Store.
func (s *Store) Terminate(ctx context.Context, instanceID, reason string) error {
	ev := store.ChildOutcome(store.Execution{Status: store.StatusTerminated, Err: reason})
	for range maxRetries {
		err := retry(ctx, func() error {
			b := &pgx.Batch{}
			b.Queue(terminate, instanceID, reason)
			b.Queue(`WITH p AS (DELETE FROM flows.pending_event WHERE execution_id = `+latestOf+`)
				DELETE FROM flows.activity_task WHERE execution_id = `+latestOf, instanceID)
			b.Queue(notifyTerminated, instanceID, ev.Err, ev.ErrKinds)
			br := s.pool.SendBatch(ctx, b)
			defer br.Close()
			var id int64
			if err := br.QueryRow().Scan(&id); err != nil {
				return err
			}
			for range 2 {
				if _, err := br.Exec(); err != nil {
					return err
				}
			}
			return br.Close()
		})
		if sqlState(err) != codeLeaseLost {
			if err != nil {
				return fmt.Errorf("terminate: %w", err)
			}
			return nil
		}
		// Nothing active was terminated: the latest execution ended, or
		// continued as new while we waited for its row (then try its
		// successor).
		e, gerr := s.GetExecution(ctx, instanceID)
		if gerr != nil {
			return gerr
		}
		if !e.Status.Active() {
			return store.ErrNotActive
		}
	}
	return fmt.Errorf("terminate: %s kept continuing as new", instanceID)
}
