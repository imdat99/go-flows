package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/imdat99/go-flows/internal/store"
)

var _ store.Watcher = (*Store)(nil)

// Watch implements store.Watcher with LISTEN on a connection taken out of
// the pool for the duration (a LISTEN must not follow a connection back to
// the pool).
func (s *Store) Watch(ctx context.Context, wake func(kind store.WorkKind, queue string)) error {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	conn := pc.Hijack()
	defer func() {
		// Bounded: a dead peer must not hang the caller.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+workChannel); err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	// Work made ready before LISTEN took effect (a start, a restart after
	// a failure) notified nobody: poll for it once.
	wake(store.WorkflowWork, "")
	wake(store.ActivityWork, "")
	// Waiting with ctx would make pgconn watch it per notification
	// (allocations on every hint): ctx is watched once instead, its end
	// cutting the wait short with a past deadline.
	stop := context.AfterFunc(ctx, func() { _ = conn.PgConn().Conn().SetDeadline(time.Unix(1, 0)) })
	defer stop()
	for {
		n, err := conn.WaitForNotification(context.Background())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("watch: %w", err)
		}
		if len(n.Payload) < 1 {
			continue
		}
		switch n.Payload[0] {
		case 'w':
			wake(store.WorkflowWork, n.Payload[1:])
		case 'a':
			wake(store.ActivityWork, n.Payload[1:])
		}
	}
}
