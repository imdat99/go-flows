//go:build chaos

package chaos_test

import (
	"testing"
	"time"
)

// pausedOverlap on synthetic windows and attempts (no processes): ttl 3s,
// so a paused lease lapses no sooner than 2s (less lapseSlack) after its
// renewals stopped. Process 1 runs a, process 2 runs b.
func TestPausedOverlapRules(t *testing.T) {
	const ttl = 3 * time.Second
	cl := &cluster{ttl: ttl}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	win := func(from, to int) window { return window{from: at(from), to: at(to)} }
	att := func(pid, start, end int) attemptRow { return attemptRow{pid: pid, start: at(start), end: at(end)} }
	cases := []struct {
		name         string
		stops, holes map[int][]window
		a, b         attemptRow
		want         bool
	}{
		{
			// A blackhole over [0, 2.5ttl]; a claimed and started after it
			// (a fresh lease), paused at 2.7ttl; b starts at 2.71ttl, with
			// a's lease live for about 0.9ttl more: not excused.
			name:  "blackhole ended before the attempt started",
			holes: map[int][]window{1: {win(0, 7500)}},
			stops: map[int][]window{1: {win(8100, 20000)}},
			a:     att(1, 7800, 20500), b: att(2, 8130, 9000),
			want: false,
		},
		{
			name:  "blackhole ended before the pause, renewed since",
			holes: map[int][]window{1: {win(0, 1000)}},
			stops: map[int][]window{1: {win(4000, 20000)}},
			a:     att(1, -1000, 20500), b: att(2, 4500, 9000),
			want: false,
		},
		{
			// Renewals stopped at the blackhole's start (still on at the
			// pause): b may start 2s after it.
			name:  "blackhole still on at the pause",
			holes: map[int][]window{1: {win(0, 10000)}},
			stops: map[int][]window{1: {win(3000, 12000)}},
			a:     att(1, -1000, 12500), b: att(2, 3500, 9000),
			want: true,
		},
		{
			name:  "blackhole still on at the pause, b too soon after its start",
			holes: map[int][]window{1: {win(0, 10000)}},
			stops: map[int][]window{1: {win(1000, 12000)}},
			a:     att(1, -1000, 12500), b: att(2, 1500, 9000),
			want: false,
		},
		{
			// The heartbeat gave the lease up during the blackhole: never
			// renewed after it.
			name:  "long blackhole over the attempt, ended before the pause",
			holes: map[int][]window{1: {win(0, 2500)}},
			stops: map[int][]window{1: {win(2800, 12000)}},
			a:     att(1, -1000, 12500), b: att(2, 3000, 9000),
			want: true,
		},
		{
			name:  "blackhole only, no pause",
			holes: map[int][]window{1: {win(0, 10000)}},
			a:     att(1, -1000, 11000), b: att(2, 3000, 9000),
			want: false,
		},
		{
			name:  "pause only",
			stops: map[int][]window{1: {win(0, 5000)}},
			a:     att(1, -1000, 5500), b: att(2, 2000, 9000),
			want: true,
		},
		{
			name:  "pause only, b too soon",
			stops: map[int][]window{1: {win(0, 5000)}},
			a:     att(1, -1000, 5500), b: att(2, 1000, 9000),
			want: false,
		},
		{
			name:  "pause only, a ran on long after the resume",
			stops: map[int][]window{1: {win(0, 5000)}},
			a:     att(1, -1000, 9000), b: att(2, 2000, 9000),
			want: false,
		},
		{
			// b's process paused after its claim; b recorded its start on
			// resume, after a's.
			name:  "pause of b before it recorded its start",
			stops: map[int][]window{2: {win(0, 5000)}},
			a:     att(1, 2000, 9000), b: att(2, 5200, 6000),
			want: true,
		},
		{
			name:  "pause of b, a too soon",
			stops: map[int][]window{2: {win(0, 5000)}},
			a:     att(1, 1000, 9000), b: att(2, 5200, 6000),
			want: false,
		},
	}
	for _, c := range cases {
		if got := cl.pausedOverlap(c.stops, c.holes, c.a, c.b); got != c.want {
			t.Errorf("%s: excused %v, want %v", c.name, got, c.want)
		}
	}
}
