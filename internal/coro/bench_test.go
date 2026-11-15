package coro

import "testing"

// BenchmarkScheduler: 1000 coroutines each wait on their own future; one op
// sets every future and runs the scheduler (1000 hand-offs). Steady state
// must not allocate (GW-16 and the performance budget).
func BenchmarkScheduler(b *testing.B) {
	const n = 1000
	s := NewScheduler(0)
	defer s.Close()
	futures := make([]*Future[int], n)
	for i := range futures {
		futures[i] = NewFuture[int]()
	}
	cur := make([]*Future[int], n) // the future each coroutine waits on now
	copy(cur, futures)
	for i := range n {
		s.Spawn(s.Root(), func(ctx Context) error {
			for {
				f := cur[i]
				if _, err := f.Get(ctx); err != nil {
					return err
				}
				f.set = false // reuse: reset for the next round
			}
		})
	}
	if err := s.Run(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for _, f := range futures {
			f.Set(1, nil)
		}
		if err := s.Run(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/switch")
}

// BenchmarkChannelPingPong: two coroutines exchange values over unbuffered
// channels.
func BenchmarkChannelPingPong(b *testing.B) {
	s := NewScheduler(0)
	defer s.Close()
	ping, pong := NewChannel[int](0), NewChannel[int](0)
	s.Spawn(s.Root(), func(ctx Context) error {
		for {
			v, ok := ping.Receive(ctx)
			if !ok {
				return nil
			}
			_ = pong.Send(ctx, v)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	done := false
	s.Spawn(s.Root(), func(ctx Context) error {
		for i := range b.N {
			_ = ping.Send(ctx, i)
			_, _ = pong.Receive(ctx)
		}
		done = true
		return nil
	})
	if err := s.Run(); err != nil || !done {
		b.Fatal(err, done)
	}
}
