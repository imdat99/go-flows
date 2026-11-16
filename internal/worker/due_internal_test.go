package worker

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestDueTimes(t *testing.T) {
	const grain = 10 * time.Millisecond
	d := newDueTimes(grain, time.Minute)
	base := time.Now()
	var want []time.Time
	for range 500 {
		at := base.Add(time.Duration(rand.IntN(int(time.Second))))
		d.add(at)
		want = append(want, at)
	}
	d.add(base.Add(2 * time.Minute)) // beyond the horizon: dropped
	slices.SortFunc(want, time.Time.Compare)
	var prev time.Time
	n := 0
	for _, w := range want {
		got, ok := d.next(w.Add(-time.Nanosecond))
		if !ok {
			t.Fatalf("no time for %v", w)
		}
		// Rounded up by less than a grain, never early.
		if got.Before(w) || got.Sub(w) >= grain {
			t.Fatalf("time for %v is %v", w, got)
		}
		if !got.Equal(prev) {
			if !prev.IsZero() && got.Sub(prev) < grain {
				t.Fatalf("times %v and %v closer than a grain", prev, got)
			}
			prev = got
			n++
		}
		if got2, _ := d.next(time.Time{}); !got2.Equal(got) {
			t.Fatal("a zero poll time dropped a time")
		}
	}
	if n > int(time.Second/grain)+1 {
		t.Fatalf("%d distinct times for one second, want at most %d", n, time.Second/grain+1)
	}
	if _, ok := d.next(base.Add(time.Second + grain)); ok {
		t.Fatal("times left (or one beyond the horizon kept)")
	}
	if len(d.set) != 0 || len(d.h) != 0 {
		t.Fatalf("set %d heap %d", len(d.set), len(d.h))
	}
}

func TestDueTimesSignalsOnlyANewEarliest(t *testing.T) {
	d := newDueTimes(time.Millisecond, time.Minute)
	hint := d.changed
	now := time.Now()
	d.add(now.Add(time.Second))
	<-hint
	for _, later := range []time.Duration{time.Second, 2 * time.Second} { // a duplicate, a later time
		d.add(now.Add(later))
		select {
		case <-hint:
			t.Fatalf("hint for %v", later)
		default:
		}
	}
	d.add(now.Add(time.Millisecond))
	select {
	case <-hint:
	default:
		t.Fatal("no hint for a new earliest time")
	}
}
