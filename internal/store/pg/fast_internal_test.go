package pg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/store"
	"github.com/imdat99/go-flows/internal/testdb"
)

// binArray encodes a one-dimensional binary array (nil: NULL element).
func binArray(elems ...[]byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, 1)
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, oidBytea)
	b = binary.BigEndian.AppendUint32(b, uint32(len(elems)))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, e := range elems {
		if e == nil {
			b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(e)))
		b = append(b, e...)
	}
	return b
}

func TestDecodeArray(t *testing.T) {
	got, err := decodeArray(binArray([]byte("ab"), nil, []byte{}), nil)
	if err != nil || len(got) != 3 || !bytes.Equal(got[0], []byte("ab")) || got[1] != nil || got[2] == nil || len(got[2]) != 0 {
		t.Fatalf("got %q %v", got, err)
	}
	empty := binary.BigEndian.AppendUint32(nil, 0) // '{}': no dimensions
	empty = append(empty, make([]byte, 8)...)
	if got, err := decodeArray(empty, nil); err != nil || len(got) != 0 {
		t.Fatalf("empty: %q %v", got, err)
	}
	full := binArray([]byte("abc"))
	for i := range len(full) - 1 {
		if _, err := decodeArray(full[:i], nil); err == nil {
			t.Fatalf("truncated at %d: no error", i)
		}
	}
}

// T8.2: the fast path watches its context itself (connection deadlines):
// a completion blocked on a row lock returns when its context ends, by
// deadline or cancellation, and the store keeps working.
func TestFastPathHonorsContext(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := New(pool)
	if !s.fast {
		t.Skip("fast path disabled")
	}
	for i, how := range []string{"deadline", "cancel"} {
		id := "i" + strconv.Itoa(i)
		if _, _, err := s.CreateExecution(ctx, store.NewExecution{InstanceID: id, Workflow: "w", Version: 1, Queue: "q"}); err != nil {
			t.Fatal(err)
		}
		tasks, err := s.ClaimWorkflowTasks(ctx, "o", []string{"q"}, 1, time.Minute)
		if err != nil || len(tasks) != 1 {
			t.Fatalf("claim: %v %v", tasks, err)
		}
		task := tasks[0]
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM flows.execution WHERE id = $1 FOR UPDATE`, task.Execution.ID); err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithCancel(ctx)
		if how == "deadline" {
			cctx, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
		} else {
			time.AfterFunc(200*time.Millisecond, cancel)
		}
		start := time.Now()
		_, err = s.CompleteWorkflowTask(cctx, task.Lease, &store.Completion{
			History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
			Consumed: []int64{task.Pending[0].ID},
		})
		cancel()
		if err == nil || cctx.Err() == nil || !errors.Is(err, cctx.Err()) {
			t.Fatalf("%s: got %v", how, err)
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("%s: returned after %v", how, d)
		}
		_ = tx.Rollback(ctx)
		// The store still works (the interrupted connection was dropped).
		if _, err := s.CompleteWorkflowTask(ctx, task.Lease, &store.Completion{
			History:  []history.Event{{Seq: 1, Type: history.ExecutionStarted}},
			Consumed: []int64{task.Pending[0].ID},
		}); err != nil {
			t.Fatalf("%s: after the interruption: %v", how, err)
		}
	}
}

// T8.2 review: CreateExecution gets the application's context (possibly
// never canceled): it is watched for the call only, leaving nothing
// registered, and still ends the call when it ends.
func TestCreateWatchesItsContextPerCall(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	s := New(pool)
	if !s.fast {
		t.Skip("fast path disabled")
	}
	live, cancelLive := context.WithCancel(ctx) // never canceled during the test
	defer cancelLive()
	if _, _, err := s.CreateExecution(live, store.NewExecution{InstanceID: "a", Workflow: "w", Version: 1, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	n := 0
	s.watches.Range(func(any, any) bool { n++; return true })
	if n != 0 {
		t.Fatalf("%d context watches left registered", n)
	}
	// An insert of the same id, not committed: the create waits on it.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO flows.execution (instance_id, workflow, version, queue, status) VALUES ('b', 'w', 1, 'q', 'running')`); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err = s.CreateExecution(cctx, store.NewExecution{InstanceID: "b", Workflow: "w", Version: 1, Queue: "q"})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("got %v after %v", err, time.Since(start))
	}
}

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
