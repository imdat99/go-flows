package worker_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
	"github.com/imdat99/go-flows/internal/worker"
)

// T11.8: a PollMax above LeaseTTL/3 is lowered, and the engine says so once.
func TestPollMaxLoweredIsLogged(t *testing.T) {
	for _, tc := range []struct {
		pollMax, ttl time.Duration
		logged       bool
	}{
		{30 * time.Second, 9 * time.Second, true},
		{time.Second, 9 * time.Second, false},
		{0, 2 * time.Second, false}, // the default (1s) lowered: the user asked for nothing
	} {
		var buf bytes.Buffer
		_, err := worker.New(mem.New(nil), nil, nil, worker.Options{
			Owner: "o", Queues: []string{"q"}, LeaseTTL: tc.ttl, PollMax: tc.pollMax,
			Logger: slog.New(slog.NewTextHandler(&buf, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Count(buf.String(), "PollMax lowered"); got != map[bool]int{true: 1, false: 0}[tc.logged] {
			t.Fatalf("PollMax %v: logged %d times: %s", tc.pollMax, got, buf.String())
		}
	}
}
