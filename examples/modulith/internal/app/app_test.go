package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/processes/fulfilment"
	"github.com/imdat99/go-flows/examples/modulith/processes/shipment"
	"github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// engineLog records the activities (and compensators) an engine ran, and
// the workflows whose end it recorded (the engine that runs the last task).
type engineLog struct {
	mu         sync.Mutex
	activities map[string]bool
	workflows  map[string]bool
}

func (l *engineLog) ActivityAttempt(a flows.ActivityAttempt) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.activities == nil {
		l.activities = map[string]bool{}
	}
	l.activities[a.Activity] = true
}

func (l *engineLog) RunEnded(r flows.RunInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.workflows == nil {
		l.workflows = map[string]bool{}
	}
	l.workflows[r.Workflow] = true
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for n := range m {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

func (l *engineLog) list() (activities, workflows []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return sorted(l.activities), sorted(l.workflows)
}

// frontModule reports whether name (an activity or a workflow) belongs to a
// module of the front deployment.
func frontModule(name string) bool {
	return strings.HasPrefix(name, "orders.") || strings.HasPrefix(name, "notifications.")
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
	var frontLog, backLog engineLog
	front := startApp(t, pool, Config{
		Modules: []string{"orders", "notifications"}, EngineID: "front", Observer: &frontLog, Logger: quiet,
	})
	startApp(t, pool2, Config{
		Modules: []string{"payments", "inventory", "shipping"}, EngineID: "back", Observer: &backLog, Logger: quiet,
	})
	s := split{api: front.Handler(), front: front}

	var stock stockView
	code := call(t, s.api, "POST", "/inventory/sku-t/adjust",
		adjustRequest{RequestID: "seed-1", Delta: 10, Reason: "receipt"}, &stock)
	if code != http.StatusOK || stock.Available != 10 {
		t.Fatalf("adjust: %d %+v", code, stock)
	}
	// The same request again: applied once. The same id with another delta:
	// refused. Unknown fields: refused.
	code = call(t, s.api, "POST", "/inventory/sku-t/adjust",
		adjustRequest{RequestID: "seed-1", Delta: 10, Reason: "receipt"}, &stock)
	if code != http.StatusOK || stock.Available != 10 {
		t.Fatalf("adjust again: %d %+v", code, stock)
	}
	code = call(t, s.api, "POST", "/inventory/sku-t/adjust",
		adjustRequest{RequestID: "seed-1", Delta: 11, Reason: "receipt"}, nil)
	if code != http.StatusConflict {
		t.Fatalf("adjust with a reused request id: %d, want 409", code)
	}
	code = call(t, s.api, "POST", "/inventory/sku-t/adjust", map[string]any{"request_id": "x", "delta": 1, "oops": 1}, nil)
	if code != http.StatusBadRequest {
		t.Fatalf("unknown field: %d, want 400", code)
	}

	t.Run("shipped", s.shipped)
	t.Run("canceled", s.canceled)
	t.Run("out of stock", s.outOfStock)
	t.Run("cancel after pickup", s.cancelAfterPickup)
	t.Run("after retention", s.afterRetention)

	// The split held: each engine ran only its modules' activities and
	// recorded the ends of its modules' workflows.
	frontActs, frontWfs := frontLog.list()
	backActs, backWfs := backLog.list()
	for _, n := range slices.Concat(frontActs, frontWfs) {
		if !frontModule(n) {
			t.Errorf("front ran %s", n)
		}
	}
	for _, n := range slices.Concat(backActs, backWfs) {
		if frontModule(n) {
			t.Errorf("back ran %s", n)
		}
	}
	for _, want := range []struct {
		got  []string
		name string
	}{
		{backActs, "payments.capture"},
		{backActs, "inventory.reserve-line"},
		{backActs, "shipping.create-label"},
		{backWfs, "inventory.reserve"},
		{backWfs, "shipping.ship"},
		{backWfs, "inventory.adjust"},
		{frontActs, "notifications.deliver"},
		{frontActs, "orders.record"},
		{frontWfs, "orders.fulfil"},
		{frontWfs, "notifications.send"},
	} {
		if !slices.Contains(want.got, want.name) {
			t.Errorf("%s did not run where expected: front %v %v, back %v %v", want.name, frontActs, frontWfs, backActs, backWfs)
		}
	}
}

// split drives the API of the split deployment.
type split struct {
	api   http.Handler
	front *App
}

func (s split) place(t *testing.T, id string, qty int) {
	t.Helper()
	req := orders.Order{
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
func (s split) sent(t *testing.T, id, want string) {
	t.Helper()
	eventually(t, want+" for "+id, func() bool {
		got, err := s.front.svc.notifications.Sent(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Equal(got, []string{want})
	})
}

func (s split) shipped(t *testing.T) {
	s.place(t, "o-ship", 2)
	eventually(t, "the label", func() bool { return s.view(t, "o-ship").Shipment == "label_created" })
	code := call(t, s.api, "POST", "/shipments/o-ship/pickup", shipment.Pickup{Tracking: "TRK1"}, nil)
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

	// Placing it again is a no-op, other contents under its id are refused,
	// canceling it now is refused.
	s.place(t, "o-ship", 2)
	other := orders.Order{ID: "o-ship", Email: "x@example.com", Address: "Hue", Amount: 1, Lines: []orders.Line{{SKU: "sku-t", Qty: 1}}}
	if code := call(t, s.api, "POST", "/orders", other, nil); code != http.StatusConflict {
		t.Fatalf("place other contents: %d, want 409", code)
	}
	code = call(t, s.api, "POST", "/orders/o-ship/cancel", fulfilment.CancelRequest{Reason: "late"}, nil)
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
	code := call(t, s.api, "POST", "/orders/o-cancel/cancel", fulfilment.CancelRequest{Reason: "changed my mind"}, nil)
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

	// The carrier comes after the cancel: refused, return the parcel.
	code = call(t, s.api, "POST", "/shipments/o-cancel/pickup", shipment.Pickup{Tracking: "TRK-LATE"}, nil)
	if code != http.StatusConflict {
		t.Fatalf("pickup after the cancel: %d, want 409", code)
	}
	if code := call(t, s.api, "POST", "/shipments/o-none/pickup", shipment.Pickup{Tracking: "T"}, nil); code != http.StatusNotFound {
		t.Fatalf("pickup without a shipment: %d, want 404", code)
	}
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

// The carrier confirms the pickup, then the customer cancels at once. The
// 202 of the pickup means the shipments table recorded it, so however the
// cancel's arrival interleaves with the workflows (it may reach the order
// while it still waits, or after it completed), the order completes,
// captured. The interleavings themselves are forced in flowtest
// (TestFulfilCancelAfterPickup) and on Postgres (TestDecisionRace).
func (s split) cancelAfterPickup(t *testing.T) {
	s.place(t, "o-race", 1)
	eventually(t, "the label", func() bool { return s.view(t, "o-race").Shipment == "label_created" })
	if code := call(t, s.api, "POST", "/shipments/o-race/pickup", shipment.Pickup{Tracking: "TRK2"}, nil); code != http.StatusAccepted {
		t.Fatalf("pickup: %d", code)
	}
	// 202 (the order still runs: it requests the shipment's cancel, which
	// the confirmed pickup wins) or
	// 409 (it has completed already).
	code := call(t, s.api, "POST", "/orders/o-race/cancel", fulfilment.CancelRequest{Reason: "too late"}, nil)
	if code != http.StatusAccepted && code != http.StatusConflict {
		t.Fatalf("cancel: %d", code)
	}
	eventually(t, "the end", func() bool { return s.view(t, "o-race").State != "processing" })
	if v := s.view(t, "o-race"); v.State != "completed" || v.Payment != "captured" || v.Shipment != "picked_up" {
		t.Fatalf("order %+v, want completed and captured", v)
	}
	if n := s.level(t); n != 7 {
		t.Fatalf("stock %d, want 7", n)
	}
	s.sent(t, "o-race", "order_completed")
}

// The retention job purges the ended orders' workflows; placing an order
// again is still a no-op (the orders table), other contents under its id
// still a conflict, and nothing starts.
func (s split) afterRetention(t *testing.T) {
	s.front.cfg.Retention = time.Millisecond // the test's retention: everything ended
	time.Sleep(10 * time.Millisecond)
	if err := s.front.purgeOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"o-ship", "o-cancel", "o-big", "o-race"} {
		if _, err := fulfilment.Status(t.Context(), s.front.engine, id); !errors.Is(err, flows.ErrNotFound) {
			t.Fatalf("%s after the purge: %v, want not found", id, err)
		}
	}
	s.place(t, "o-ship", 2)
	other := orders.Order{ID: "o-cancel", Email: "x@example.com", Address: "Hue", Amount: 1, Lines: []orders.Line{{SKU: "sku-t", Qty: 1}}}
	if code := call(t, s.api, "POST", "/orders", other, nil); code != http.StatusConflict {
		t.Fatalf("place other contents after the purge: %d, want 409", code)
	}
	if _, err := fulfilment.Status(t.Context(), s.front.engine, "o-ship"); !errors.Is(err, flows.ErrNotFound) {
		t.Fatalf("o-ship placed again after the purge: %v, want nothing started", err)
	}
	// The order's view survives its saga: the module's state is final.
	if v := s.view(t, "o-ship"); v.State != "completed" || v.Status != "" || v.Shipment != "picked_up" {
		t.Fatalf("order %+v, want still completed, no saga status", v)
	}
	if code := call(t, s.api, "GET", "/orders/o-none", nil, nil); code != http.StatusNotFound {
		t.Fatalf("unknown order: %d, want 404", code)
	}
}
