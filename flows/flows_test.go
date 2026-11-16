package flows

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

var bg = context.Background()

// testEngine starts an engine on a memory store, stopped with the test.
func testEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := newEngine(mem.New(nil), Options{InstanceID: "test", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		defer cancel()
		_ = e.Stop(ctx)
	})
	return e
}

func waitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type order struct {
	ID     string `json:"id"`
	Amount int64  `json:"amount"` // above 2^53: JSON numbers into typed fields keep precision (GW-18)
}

type receipt struct {
	Charge string `json:"charge"`
	Ship   string `json:"ship"`
}

var (
	charge = DefineActivity("test.charge", func(_ context.Context, k Key, o order) (string, int64, error) {
		return "ch-" + o.ID + "-" + strconv.FormatInt(o.Amount, 10), o.Amount, nil
	}, nil)
	ship = DefineActivity("test.ship", func(_ context.Context, _ Key, id string) (string, struct{}, error) {
		return "sh-" + id, struct{}{}, nil
	}, nil)
	orderWF = DefineWorkflow("test.order", 1, func(c Ctx, o order) (receipt, error) {
		ch, err := charge.Run(c, o)
		if err != nil {
			return receipt{}, err
		}
		sh, err := ship.Run(c, o.ID)
		return receipt{Charge: ch, Ship: sh}, err
	})
)

func TestRunWorkflowWithActivities(t *testing.T) {
	e := testEngine(t)
	o := order{ID: "o1", Amount: 1<<60 + 1}
	r, err := orderWF.Run(waitCtx(t), e, "order-1", o)
	if err != nil {
		t.Fatal(err)
	}
	if want := (receipt{Charge: "ch-o1-" + strconv.FormatInt(o.Amount, 10), Ship: "sh-o1"}); r != want {
		t.Fatalf("receipt %+v, want %+v", r, want)
	}
	// The instance id is an idempotency key: the same call returns the same
	// result without running anything again.
	again, err := orderWF.Run(waitCtx(t), e, "order-1", order{ID: "other"})
	if err != nil || again != r {
		t.Fatalf("second run %+v %v", again, err)
	}
}

var other = DefineWorkflow("test.other", 1, func(Ctx, int) (int, error) { return 0, nil })

func TestInstanceIDBelongsToOneWorkflow(t *testing.T) {
	e := testEngine(t)
	if _, err := orderWF.Start(bg, e, "shared", order{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Start(bg, e, "shared", 1); !errors.Is(err, ErrInstanceConflict) {
		t.Fatalf("id of another workflow: %v", err)
	}
	if _, err := other.Start(bg, e, "", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
}

var errOutOfStock = errors.New("out of stock")

func init() { RegisterErrorKind("test_out_of_stock", errOutOfStock) }

var (
	reserve = DefineActivity("test.reserve", func(context.Context, Key, int) (int, struct{}, error) {
		return 0, struct{}{}, Permanent(fmt.Errorf("item 7: %w", errOutOfStock))
	}, nil)
	reserveWF = DefineWorkflow("test.reserve-wf", 1, func(c Ctx, n int) (string, error) {
		_, err := reserve.Run(c, n)
		if !errors.Is(err, errOutOfStock) || !IsPermanent(err) {
			return "", fmt.Errorf("activity error lost its kind: %w", err)
		}
		return "", err // the workflow fails with it
	})
)

// Sentinel errors survive the history: in the workflow (on every replay)
// and in the result of the run.
func TestErrorKindsSurviveTheHistory(t *testing.T) {
	e := testEngine(t)
	_, err := reserveWF.Run(waitCtx(t), e, "reserve-1", 7)
	var re *RunError
	if !errors.As(err, &re) || re.Status != StatusFailed {
		t.Fatalf("run error %v", err)
	}
	if !errors.Is(err, errOutOfStock) {
		t.Fatalf("errors.Is lost the kind: %v (%v)", err, re.Cause.kinds)
	}
}

var awaitWF = DefineWorkflow("test.await", 1, func(c Ctx, _ struct{}) (string, error) {
	v, err := Await[string](c, "approve", time.Hour)
	if err != nil {
		return "", err
	}
	_, err = Await[string](c, "never", 50*time.Millisecond)
	if !errors.Is(err, ErrAwaitTimeout) {
		return "", fmt.Errorf("await timeout: %w", err)
	}
	return v, nil
})

func TestAwaitSignalAndTimeout(t *testing.T) {
	e := testEngine(t)
	h, err := awaitWF.Start(bg, e, "await-1", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := e.Signal(bg, "await-1", "approve", "yes"); err != nil {
		t.Fatal(err)
	}
	if v, err := h.Wait(waitCtx(t)); err != nil || v != "yes" {
		t.Fatalf("result %q %v", v, err)
	}
}

var (
	childWF  = DefineWorkflow("test.child", 1, func(_ Ctx, n int) (int, error) { return n * 10, nil })
	parentWF = DefineWorkflow("test.parent", 1, func(c Ctx, n int) (int, error) {
		a := childWF.Child(c, "child-a-"+strconv.Itoa(n), n)
		b := childWF.Child(c, "child-b-"+strconv.Itoa(n), n+1)
		x, err := a.Get(c)
		if err != nil {
			return 0, err
		}
		y, err := b.Get(c)
		return x + y, err
	})
)

var dupWF = DefineWorkflow("test.dup-parent", 1, func(c Ctx, _ int) (int, error) {
	_, err := childWF.Child(c, "child-a-1", 5).Get(c)
	if !errors.Is(err, ErrInstanceExists) {
		return 0, fmt.Errorf("duplicate child: %w", err)
	}
	return 1, nil
})

func TestChildWorkflows(t *testing.T) {
	e := testEngine(t)
	if v, err := parentWF.Run(waitCtx(t), e, "parent-1", 1); err != nil || v != 30 {
		t.Fatalf("parent result %d %v", v, err)
	}
	// A child id already in use fails the child, not the engine.
	if v, err := dupWF.Run(waitCtx(t), e, "dup-parent", 0); err != nil || v != 1 {
		t.Fatalf("dup parent %d %v", v, err)
	}
}

var countWF = DefineWorkflow("test.count", 1, func(c Ctx, n int) (int, error) {
	if n < 5 {
		return 0, ContinueAsNew(c, n+1)
	}
	return n, nil
})

func TestContinueAsNew(t *testing.T) {
	e := testEngine(t)
	if v, err := countWF.Run(waitCtx(t), e, "count-1", 0); err != nil || v != 5 {
		t.Fatalf("result %d %v", v, err)
	}
}

var sideCalls atomic.Int32

var sideWF = DefineWorkflow("test.side", 1, func(c Ctx, _ int) (string, error) {
	id, err := SideEffect(c, func() string { sideCalls.Add(1); return "gen-" + strconv.Itoa(int(sideCalls.Load())) })
	if err != nil {
		return "", err
	}
	if _, err := Rand(c); err != nil {
		return "", err
	}
	if err := Sleep(c, 10*time.Millisecond); err != nil { // a second task: replay or cache
		return "", err
	}
	again, err := SideEffect(c, func() string { return "second" })
	return id + "/" + again, err
})

func TestSideEffects(t *testing.T) {
	sideCalls.Store(0)
	e := testEngine(t)
	v, err := sideWF.Run(waitCtx(t), e, "side-1", 0)
	if err != nil || v != "gen-1/second" || sideCalls.Load() != 1 {
		t.Fatalf("result %q %v, side effect ran %d times", v, err, sideCalls.Load())
	}
}

var selectWF = DefineWorkflow("test.select", 1, func(c Ctx, _ int) (string, error) {
	ch := Signals[string](c, "pick")
	got := ""
	tc, cancel := WithCancel(c)
	timer := NewTimer(tc, time.Hour)
	Select(c,
		OnSignal(ch, func(_ Ctx, v string, err error) { got = v }),
		OnFuture(timer, func(Ctx, struct{}, error) { got = "timeout" }),
	)
	cancel()
	return got, nil
})

func TestSelect(t *testing.T) {
	e := testEngine(t)
	h, _ := selectWF.Start(bg, e, "select-1", 0)
	time.Sleep(30 * time.Millisecond)
	_ = e.Signal(bg, "select-1", "pick", "signal")
	if v, err := h.Wait(waitCtx(t)); err != nil || v != "signal" {
		t.Fatalf("result %q %v", v, err)
	}
}

var panicWF = DefineWorkflow("test.panic", 1, func(Ctx, int) (int, error) { panic("boom") })

func TestWorkflowPanicFailsWithErrPanic(t *testing.T) {
	e := testEngine(t)
	_, err := panicWF.Run(waitCtx(t), e, "panic-1", 0)
	if !errors.Is(err, ErrPanic) {
		t.Fatalf("panic run: %v", err)
	}
}

var waitForeverWF = DefineWorkflow("test.forever", 1, func(c Ctx, _ int) (int, error) {
	_, err := Signals[int](c, "never").Receive(c)
	return 0, err
})

func TestCancelAndTerminate(t *testing.T) {
	e := testEngine(t)
	h1, _ := waitForeverWF.Start(bg, e, "cancel-1", 0)
	h2, _ := waitForeverWF.Start(bg, e, "term-1", 0)
	time.Sleep(30 * time.Millisecond)
	if err := e.Cancel(bg, "cancel-1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Terminate(bg, "term-1", "stuck"); err != nil {
		t.Fatal(err)
	}
	var re *RunError
	if _, err := h1.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusCanceled {
		t.Fatalf("canceled: %v", err)
	}
	if _, err := h2.Wait(waitCtx(t)); !errors.As(err, &re) || re.Status != StatusTerminated || !errors.Is(err, ErrTerminated) {
		t.Fatalf("terminated: %v", err)
	}
}

func TestDefineRejectsBadNames(t *testing.T) {
	for name, define := range map[string]func(){
		"empty": func() { DefineWorkflow("", 1, func(Ctx, int) (int, error) { return 0, nil }) },
		"reserved": func() {
			DefineActivity("flows.x", func(context.Context, Key, int) (int, int, error) { return 0, 0, nil }, nil)
		},
		"duplicate": func() { DefineWorkflow("test.order", 1, func(Ctx, order) (receipt, error) { return receipt{}, nil }) },
		"version 0": func() { DefineWorkflow("test.v0", 0, func(Ctx, int) (int, error) { return 0, nil }) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			define()
		}()
	}
}

var (
	nanWF = DefineWorkflow("test.nan", 1, func(c Ctx, _ int) (float64, error) {
		return SideEffect(c, math.NaN) // not encodable
	})
	firstRand atomic.Uint64
	randWF    = DefineWorkflow("test.rand", 1, func(c Ctx, _ int) (uint64, error) {
		r, err := Rand(c)
		if err != nil {
			return 0, err
		}
		firstRand.CompareAndSwap(0, r)
		if err := Sleep(c, 5*time.Millisecond); err != nil { // a second task, replayed (no cache)
			return 0, err
		}
		return r, nil
	})
)

// A side effect that cannot be encoded fails the workflow (it would not
// replay); Rand replays its recorded value.
func TestSideEffectEncodingAndRandReplay(t *testing.T) {
	e, err := newEngine(mem.New(nil), Options{InstanceID: "nocache", CacheSize: -1, PollMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(bg) })
	if _, err := nanWF.Run(waitCtx(t), e, "nan-1", 0); !errors.Is(err, ErrPanic) {
		t.Fatalf("unencodable side effect: %v", err)
	}
	firstRand.Store(0)
	r, err := randWF.Run(waitCtx(t), e, "rand-1", 0)
	if err != nil || r != firstRand.Load() {
		t.Fatalf("rand %d, first seen %d, %v", r, firstRand.Load(), err)
	}
}

var (
	slowAct = DefineActivity("test.slow", func(ctx context.Context, _ Key, _ int) (int, struct{}, error) {
		<-ctx.Done()
		return 0, struct{}{}, ctx.Err()
	}, nil, WithTimeout(20*time.Millisecond), WithRetry(RetryPolicy{Attempts: 1}))
	edgeWF = DefineWorkflow("test.edges", 1, func(c Ctx, _ int) (string, error) {
		// Timeout: uncertain, ErrTimeout.
		_, err := slowAct.Run(c, 0)
		var re *RecordedError
		if !errors.Is(err, ErrTimeout) || !errors.As(err, &re) || !re.Uncertain {
			return "", fmt.Errorf("timeout: %w", err)
		}
		// A child with an invalid id fails before anything is issued; Select
		// still sees its failure.
		bad := childWF.Child(c, "", 1)
		var selErr error
		Select(c, OnFuture(bad, func(_ Ctx, _ int, e error) { selErr = e }), SelectCase{})
		if !errors.Is(selErr, ErrInvalidArgument) {
			return "", fmt.Errorf("invalid child id: %w", selErr)
		}
		// Await canceled by its parent context.
		ac, cancel := WithCancel(c)
		Go(c, func(c Ctx) { _ = Sleep(c, 10*time.Millisecond); cancel() })
		if _, err := Await[int](ac, "never", time.Hour); !errors.Is(err, ErrCanceled) {
			return "", fmt.Errorf("await canceled: %w", err)
		}
		var zero Future[int]
		if _, err := zero.Get(c); err == nil {
			return "", errors.New("zero future")
		}
		return "ok", nil
	})
)

func TestEdgeCases(t *testing.T) {
	e := testEngine(t)
	if v, err := edgeWF.Run(waitCtx(t), e, "edges-1", 0); err != nil || v != "ok" {
		t.Fatalf("%q %v", v, err)
	}
	// The workflow failure carries permanence; a canceled run matches
	// ErrCanceled.
	_, err := reserveWF.Run(waitCtx(t), e, "reserve-perm", 1)
	if !IsPermanent(err) {
		t.Fatalf("permanent kind lost on the run error: %v", err)
	}
	h, _ := waitForeverWF.Start(bg, e, "cancel-kind", 0)
	time.Sleep(20 * time.Millisecond)
	_ = e.Cancel(bg, "cancel-kind")
	if _, err := h.Wait(waitCtx(t)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled run: %v", err)
	}
}

func TestRegisterErrorKindValidates(t *testing.T) {
	for _, name := range []string{"", "a,b", "a b", "permanent", "test_out_of_stock", "bad\x00"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%q accepted", name)
				}
			}()
			RegisterErrorKind(name, errors.New(name))
		}()
	}
}

// GW-18: integers beyond 2^53 inside interface values keep their value.
func TestCodecKeepsLargeIntegersInInterfaces(t *testing.T) {
	cd := newCodec[map[string]any]()
	b, _ := cd.encode(map[string]any{"n": uint64(1<<60 + 1)})
	v, err := cd.decode(b)
	if err != nil || fmt.Sprint(v["n"]) != strconv.FormatUint(1<<60+1, 10) {
		t.Fatalf("decoded %v %v", v, err)
	}
}

func TestCodecInterfacePathRejectsTrailingData(t *testing.T) {
	cd := newCodec[map[string]any]()
	if _, err := cd.decode([]byte(`{"a":1} {"b":2}`)); err == nil {
		t.Fatal("trailing value accepted")
	}
	if v, err := cd.decode([]byte("{\"a\":1}\n")); err != nil || v["a"] == nil {
		t.Fatalf("trailing whitespace rejected: %v %v", v, err)
	}
}

func TestZeroCtxPanicsWithErrNotInWorkflow(t *testing.T) {
	var z Ctx
	calls := map[string]func(){
		"Value":        func() { z.Value("k") },
		"WithValue":    func() { WithValue(z, "k", 1) },
		"WithCancel":   func() { WithCancel(z) },
		"Disconnected": func() { Disconnected(z) },
		"Go":           func() { Go(z, func(Ctx) {}) },
		"Select":       func() { Select(z, OnDefault(func(Ctx) {})) },
	}
	for name, call := range calls {
		func() {
			defer func() {
				r := recover()
				if e, ok := r.(error); !ok || !errors.Is(e, ErrNotInWorkflow) {
					t.Errorf("%s: recovered %v", name, r)
				}
			}()
			call()
		}()
	}
}

// A WaitGroup can be reused once its counter reached zero; Wait at zero
// returns nil.
var reuseWG = DefineWorkflow("test.reuse-wg", 1, func(c Ctx, _ int) (int, error) {
	var wg WaitGroup
	n := 0
	for range 2 {
		wg.Add(1)
		Go(c, func(Ctx) { n++; wg.Done() })
		if err := wg.Wait(c); err != nil {
			return 0, err
		}
		if err := Sleep(c, time.Millisecond); err != nil {
			return 0, err
		}
	}
	return n, wg.Wait(c)
})

func TestWaitGroupReuse(t *testing.T) {
	e := testEngine(t)
	if n, err := reuseWG.Run(waitCtx(t), e, "reuse-wg", 0); err != nil || n != 2 {
		t.Fatalf("%d %v", n, err)
	}
}

// WaitGroup.Wait returns ErrCanceled when the workflow is canceled first.
var cancelWG = DefineWorkflow("test.cancel-wg", 1, func(c Ctx, _ int) (string, error) {
	var wg WaitGroup
	wg.Add(1)
	dc, _ := Disconnected(c)
	Go(dc, func(c Ctx) {
		defer wg.Done()
		_ = Sleep(c, time.Hour)
	})
	if err := wg.Wait(c); errors.Is(err, ErrCanceled) {
		return "canceled", nil
	}
	return "not canceled", nil
})

func TestWaitGroupWaitHonorsCancel(t *testing.T) {
	e := testEngine(t)
	h, err := cancelWG.Start(bg, e, "cancel-wg", 0)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return historyHas(e, "cancel-wg", "TimerScheduled") })
	if err := e.Cancel(bg, "cancel-wg"); err != nil {
		t.Fatal(err)
	}
	if out, err := h.Wait(waitCtx(t)); err != nil || out != "canceled" {
		t.Fatalf("%q %v", out, err)
	}
}
