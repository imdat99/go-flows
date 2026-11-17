package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/go-flows/examples/modulith/modules/inventory"
	"github.com/imdat99/go-flows/examples/modulith/modules/orders"
	"github.com/imdat99/go-flows/examples/modulith/modules/shipping"
	"github.com/imdat99/go-flows/examples/modulith/processes/fulfilment"
	"github.com/imdat99/go-flows/examples/modulith/processes/replenishment"
	"github.com/imdat99/go-flows/examples/modulith/processes/shipment"
	"github.com/imdat99/go-flows/flows"
)

// maxBody bounds request bodies: the engine stores inputs and signals as
// they are, so the API is where their size is limited.
const maxBody = 64 << 10

// adjustWait bounds how long POST /inventory/{sku}/adjust waits for the
// adjustment (it may queue behind a replenishment cycle). Past it the
// client gets 504 and retries with the same request_id. Keep it below the
// server's WriteTimeout.
const adjustWait = 15 * time.Second

// Handler returns the HTTP API. It works in any process, whatever modules
// it runs: it only starts workflows, sends signals and reads.
//
//	POST /orders                      place an order (orders.Order)
//	GET  /orders/{id}                 the order, its payment and its shipment
//	POST /orders/{id}/cancel          {"reason": "..."}
//	POST /shipments/{order}/pickup    the carrier's webhook: {"tracking": "..."}
//	GET  /inventory/{sku}             the stock level
//	POST /inventory/{sku}/adjust      {"request_id": "...", "delta": 5, "reason": "..."}
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.placeOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /orders/{id}/cancel", a.cancelOrder)
	mux.HandleFunc("POST /shipments/{order}/pickup", a.confirmPickup)
	mux.HandleFunc("GET /inventory/{sku}", a.getStock)
	mux.HandleFunc("POST /inventory/{sku}/adjust", a.adjustStock)
	return mux
}

// placeOrder starts the order saga (the process layer).
func (a *App) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req orders.Order
	if !a.decode(w, r, &req) {
		return
	}
	if err := fulfilment.Place(r.Context(), a.engine, req); err != nil {
		a.fail(w, r, err)
		return
	}
	a.reply(w, http.StatusAccepted, map[string]string{"id": req.ID})
}

// orderView composes the saga's status (the engine) and the views of three
// modules, each through its own service.
type orderView struct {
	orders.View
	// Status is the saga's status (running, completed, compensating,
	// compensated, needs_attention...); empty once retention purged it.
	Status   flows.Status `json:"status,omitempty"`
	Payment  string       `json:"payment,omitempty"`
	Shipment string       `json:"shipment,omitempty"`
}

func (a *App) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := fulfilment.Status(r.Context(), a.engine, id)
	if err != nil && !errors.Is(err, flows.ErrNotFound) {
		a.fail(w, r, err)
		return
	}
	v, found, gerr := a.svc.orders.Get(r.Context(), id) // State "" before the saga recorded it
	switch {
	case gerr != nil:
		a.fail(w, r, gerr)
		return
	case err != nil && !found: // no saga, no order
		a.fail(w, r, err)
		return
	}
	// No saga but an order: retention purged the saga; the order's state
	// is final. Status is empty.
	out := orderView{View: v, Status: st}
	if p, err := a.svc.payments.Find(r.Context(), id); err == nil {
		out.Payment = p.State
	} else if !errors.Is(err, pgx.ErrNoRows) {
		a.fail(w, r, err)
		return
	}
	if s, err := a.svc.shipping.Find(r.Context(), id); err == nil {
		out.Shipment = s
	} else if !errors.Is(err, pgx.ErrNoRows) {
		a.fail(w, r, err)
		return
	}
	a.reply(w, http.StatusOK, out)
}

func (a *App) cancelOrder(w http.ResponseWriter, r *http.Request) {
	var req fulfilment.CancelRequest
	if !a.decode(w, r, &req) {
		return
	}
	if err := fulfilment.Cancel(r.Context(), a.engine, r.PathValue("id"), req.Reason); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// confirmPickup is the carrier's webhook. The shipping module decides
// (the pickup is recorded for good, or refused); only then is the shipment
// workflow woken up. 202 means the pickup is recorded: the order will
// complete. If waking fails, the client retries: both steps are idempotent,
// and the workflow also finds the decision when its timeout fires.
func (a *App) confirmPickup(w http.ResponseWriter, r *http.Request) {
	var req shipment.Pickup
	if !a.decode(w, r, &req) {
		return
	}
	order := r.PathValue("order")
	if err := a.svc.shipping.ConfirmPickup(r.Context(), order, req.Tracking); err != nil {
		a.fail(w, r, err)
		return
	}
	if err := shipment.WakeOnPickup(r.Context(), a.engine, order, req.Tracking); err != nil {
		a.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type stockView struct {
	SKU       string `json:"sku"`
	Available int    `json:"available"`
}

func (a *App) getStock(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	n, err := a.svc.inventory.Level(r.Context(), sku)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.reply(w, http.StatusOK, stockView{SKU: sku, Available: n})
}

type adjustRequest struct {
	RequestID string `json:"request_id"` // the client's idempotency key
	Delta     int    `json:"delta"`
	Reason    string `json:"reason"`
}

func (a *App) adjustStock(w http.ResponseWriter, r *http.Request) {
	var req adjustRequest
	if !a.decode(w, r, &req) {
		return
	}
	if req.RequestID == "" || req.Delta == 0 {
		a.reply(w, http.StatusBadRequest, map[string]string{"error": "request_id and a non-zero delta are required"})
		return
	}
	sku := r.PathValue("sku")
	ctx, cancel := context.WithTimeout(r.Context(), adjustWait)
	defer cancel()
	n, err := replenishment.Adjust(ctx, a.engine, req.RequestID, sku, req.Delta, req.Reason)
	if errors.Is(err, context.DeadlineExceeded) {
		a.reply(w, http.StatusGatewayTimeout, map[string]string{"error": "still running: retry with the same request_id"})
		return
	}
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.reply(w, http.StatusOK, stockView{SKU: sku, Available: n})
}

// decode reads one JSON object with known fields only, and nothing after it.
func (a *App) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("unexpected data after the JSON object")
	}
	if err != nil {
		a.reply(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return false
	}
	return true
}

func (a *App) reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// fail maps the modules' and the engine's errors to HTTP statuses. Errors read back from a
// workflow's history keep their kinds (flows.RegisterErrorKind), so
// errors.Is works on them too.
func (a *App) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, orders.ErrInvalidOrder), errors.Is(err, flows.ErrInvalidArgument):
		status = http.StatusBadRequest
	case errors.Is(err, flows.ErrNotFound), errors.Is(err, inventory.ErrUnknownSKU),
		errors.Is(err, shipping.ErrNoShipment):
		status = http.StatusNotFound
	case errors.Is(err, flows.ErrNotActive), errors.Is(err, flows.ErrInstanceConflict),
		errors.Is(err, inventory.ErrOutOfStock), errors.Is(err, orders.ErrOrderConflict),
		errors.Is(err, inventory.ErrRequestConflict), errors.Is(err, shipping.ErrPickupRefused):
		status = http.StatusConflict
	}
	msg := err.Error()
	if status == http.StatusInternalServerError {
		// Details go to the log, not to the client.
		a.log.LogAttrs(r.Context(), slog.LevelError, "modulith: request failed",
			slog.String("path", r.URL.Path), slog.Any("error", err))
		msg = "internal error"
	}
	a.reply(w, status, map[string]string{"error": msg})
}
