package worker

import (
	"context"
	"testing"
)

// T10.21: an activity claim holds the slots it takes for its round trip,
// so it leaves one free when more than one is: an eager launch meanwhile
// finds it (otherwise its activity goes through the queue). A single free
// slot is taken (the poller never waits for two).
func TestAcquireLeavesASpareSlot(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct{ held, spare, want int }{
		{0, 0, 4},
		{2, 0, 2},
		{3, 0, 1},
		{0, 1, 3},
		{1, 1, 2},
		{2, 1, 1},
		{3, 1, 1},
	} {
		slots := make(chan struct{}, 4)
		for range c.held {
			slots <- struct{}{}
		}
		if n := acquire(ctx, slots, c.spare); n != c.want {
			t.Errorf("%d of 4 held, spare %d: took %d, want %d", c.held, c.spare, n, c.want)
		}
	}
}
