package pg

import (
	"testing"
	"time"
)

// Binary timestamps round-trip at microsecond precision across the whole
// range Postgres stores, including beyond time.Duration's 292 years.
func TestTimestampRoundTrip(t *testing.T) {
	for _, ts := range []time.Time{
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1999, 12, 31, 23, 59, 59, 999_999_000, time.UTC),
		time.Date(2026, 11, 15, 16, 30, 0, 123_456_000, time.UTC),
		time.Date(1700, 3, 1, 1, 2, 3, 4_000, time.UTC),
		time.Date(2400, 6, 1, 0, 0, 0, 1_000, time.UTC),
	} {
		var b [8]byte
		us := tstzMicros(ts)
		for i := range 8 {
			b[i] = byte(uint64(us) >> (56 - 8*i))
		}
		got, err := decodeTime(b[:])
		if err != nil || !got.Equal(ts) {
			t.Errorf("%v round-tripped to %v (%v)", ts, got, err)
		}
	}
}
