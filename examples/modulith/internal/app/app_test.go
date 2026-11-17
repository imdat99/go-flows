package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/modules/notifications"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// activityLog records the activities (and compensators) an engine ran.
type activityLog struct {
	mu    sync.Mutex
	names map[string]bool
}

func (l *activityLog) ActivityAttempt(a flows.ActivityAttempt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.names == nil {
		l.names = map[string]bool{}
	}
	l.names[a.Activity] = true
}

func (*activityLog) RunEnded(flows.RunInfo) {}

func (l *activityLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.names))
	for n := range l.names {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

func startApp(t *testing.T, pool *pgxpool.Pool, cfg Config) *App {
	t.Helper()
	a, err := New(t.Context(), pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return a
}

// call sends a request to h and decodes the JSON reply into out (if any).
func call(t *testing.T, h http.Handler, method, path string, body, out any) int {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		if b, err = json.Marshal(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewReader(b)))
	if out != nil && rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
			t.Fatalf("%s %s: %v: %s", method, path, err, rec.Body)
		}
	}
	return rec.Code
}

// eventually polls cond until it holds, for at most 30s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSplitDeployment runs the monolith as two deployments on one
// database: "front" runs orders and notifications, "back" runs payments,
// inventory and shipping. The HTTP API is served by front. Every order
// crosses both: the fulfilment runs on front, its payments, reservations
// and shipments on back, and the compensations of back's activities run on
// back too.
func TestSplitDeployment(t *testing.T) {
	pool := testdb.New(t)
	// Each deployment has its own pool, as separate processes would. (The
	// modules' Configure is per process; in this test the second New wins,
	// which changes nothing: both pools reach the same database.)
	pool2, err := pgxpool.New(t.Context(), testdb.DSN(pool))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool2.Close)

	// Logs are discarded: the engines may log after the test has ended.
	quiet := slog.New(slog.DiscardHandler)
	var frontLog, backLog activityLog
	front := startApp(t, pool, Config{
		Modules: []string{"orders", "notifications"}, EngineID: "front", Observer: &frontLog, Logger: quiet,
	})
	startApp(t, pool2, Config{
		Modules: []string{"payments", "inventory", "shipping"}, EngineID: "back", Observer: &backLog, Logger: quiet,
	})
	s := split{api: front.Handler()}

	var stock stockView
	code := call(t, s.api, "POST", "/inventory/sku-t/adjust",
		adjustRequest{RequestID: "seed-1", Delta: 10, Reason: "receipt"}, &stock)
	if code != http.StatusOK || stock.Available != 10 {
		t.Fatalf("adjust: %d %+v", code, stock)
	}

	t.Run("shipped", s.shipped)
	t.Run("canceled", s.canceled)
	t.Run("out of stock", s.outOfStock)

	// The split held: each engine ran only its modules' activities.
	for _, n := range frontLog.list() {
		if !strings.HasPrefix(n, "orders.") && !strings.HasPrefix(n, "notifications.") {
			t.Errorf("front ran %s", n)
		}
	}
	for _, n := range backLog.list() {
		if strings.HasPrefix(n, "orders.") || strings.HasPrefix(n, "notifications.") {
			t.Errorf("back ran %s", n)
		}
	}
	if !slices.Contains(backLog.list(), "payments.capture") || !slices.Contains(frontLog.list(), "notifications.deliver") {
		t.Errorf("front ran %v, back ran %v", frontLog.list(), backLog.list())
	}
}

// split drives the API of the split deployment.
type split struct{ api http.Handler }

func (s split) place(t *testing.T, id string, qty int) {
	t.Helper()
	req := orders.PlaceRequest{
		ID: id, Email: id + "@example.com", Address: "Hanoi", Amount: 1500,
		Lines: []orders.Line{{SKU: "sku-t", Qty: qty}},
	}
	if code := call(t, s.api, "POST", "/orders", req, nil); code != http.StatusAccepted {
		t.Fatalf("place %s: %d", id, code)
	}
}

func (s split) view(t *testing.T, id string) orderView {
	t.Helper()
	var v orderView
	if code := call(t, s.api, "GET", "/orders/"+id, nil, &v); code != http.StatusOK {
		t.Fatalf("get %s: %d", id, code)
	}
	return v
}

func (s split) level(t *testing.T) int {
	t.Helper()
	var v stockView
	call(t, s.api, "GET", "/inventory/sku-t", nil, &v)
	return v.Available
}

// sent waits until the order's only notification is want.
func (split) sent(t *testing.T, id, want string) {
	t.Helper()
	eventually(t, want+" for "+id, func() bool {
		got, err := notifications.Sent(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Equal(got, []string{want})
	})
}

func (s split) shipped(t *testing.T) {
	s.place(t, "o-ship", 2)
	eventually(t, "the label", func() bool { return s.view(t, "o-ship").Shipment == "label_created" })
	code := call(t, s.api, "POST", "/shipments/o-ship/pickup", shipping.Pickup{Tracking: "TRK1"}, nil)
	if code != http.StatusAccepted {
		t.Fatalf("pickup: %d", code)
	}
	eventually(t, "completion", func() bool { return s.view(t, "o-ship").State == "completed" })
	if v := s.view(t, "o-ship"); v.Status != flows.StatusCompleted || v.Payment != "captured" || v.Shipment != "picked_up" {
		t.Fatalf("order %+v", v)
	}
	if n := s.level(t); n != 8 {
		t.Fatalf("stock %d, want 8", n)
	}
	s.sent(t, "o-ship", "order_completed")

	// Placing it again is a no-op; canceling it now is refused.
	s.place(t, "o-ship", 2)
	code = call(t, s.api, "POST", "/orders/o-ship/cancel", orders.CancelRequest{Reason: "late"}, nil)
	if code != http.StatusConflict {
		t.Fatalf("cancel after the end: %d, want 409", code)
	}
}

func (s split) canceled(t *testing.T) {
	s.place(t, "o-cancel", 3)
	eventually(t, "the label", func() bool { return s.view(t, "o-cancel").Shipment == "label_created" })
	if n := s.level(t); n != 5 {
		t.Fatalf("stock %d, want 5 while reserved", n)
	}
	code := call(t, s.api, "POST", "/orders/o-cancel/cancel", orders.CancelRequest{Reason: "changed my mind"}, nil)
	if code != http.StatusAccepted {
		t.Fatalf("cancel: %d", code)
	}
	eventually(t, "the cancellation", func() bool { return s.view(t, "o-cancel").State == "canceled" })
	v := s.view(t, "o-cancel")
	if v.Status != flows.StatusCompensated || v.Payment != "refunded" || v.Shipment != "canceled" ||
		!strings.Contains(v.Reason, "changed my mind") {
		t.Fatalf("order %+v", v)
	}
	if n := s.level(t); n != 8 {
		t.Fatalf("stock %d, want 8 after the release", n)
	}
	s.sent(t, "o-cancel", "order_canceled")
}

func (s split) outOfStock(t *testing.T) {
	s.place(t, "o-big", 100)
	eventually(t, "the failure", func() bool { return s.view(t, "o-big").State == "failed" })
	v := s.view(t, "o-big")
	if v.Payment != "voided" || v.Shipment != "" || !strings.Contains(v.Reason, "out of stock") {
		t.Fatalf("order %+v", v)
	}
	s.sent(t, "o-big", "order_failed")
}
