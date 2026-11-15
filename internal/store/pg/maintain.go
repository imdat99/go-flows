package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

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
		SELECT id, true FROM renewed
		UNION ALL
		SELECT id, false FROM held WHERE id NOT IN (SELECT id FROM renewed)`
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
			RETURNING t.id
		)
		SELECT id, true FROM renewed
		UNION ALL
		SELECT id, false FROM held WHERE id NOT IN (SELECT id FROM renewed)`
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
	lost, busy, err := readRenewal(br, wIDs)
	if err != nil {
		return r, fmt.Errorf("renew leases: %w", err)
	}
	r.LostWorkflows, r.BusyWorkflows = append(r.LostWorkflows, lost...), busy
	if lost, busy, err = readRenewal(br, aIDs); err != nil {
		return r, fmt.Errorf("renew leases: %w", err)
	}
	r.LostActivities, r.BusyActivities = append(r.LostActivities, lost...), busy
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

func readRenewal(br pgx.BatchResults, ids []int64) (lost, busy []int64, err error) {
	rows, err := br.Query()
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	seen := make(map[int64]bool, len(ids))
	for rows.Next() {
		var id int64
		var renewed bool
		if err := rows.Scan(&id, &renewed); err != nil {
			return nil, nil, err
		}
		seen[id] = true
		if !renewed {
			busy = append(busy, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	for _, id := range ids {
		if !seen[id] {
			lost = append(lost, id)
		}
	}
	return lost, busy, nil
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
