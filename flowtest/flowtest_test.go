package flowtest_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/flowtest"
)

var (
	errDeclined = errors.New("declined")
	refunds     atomic.Int32

	fetch = flows.DefineActivity("ft.fetch", func(context.Context, flows.Key, string) (string, struct{}, error) {
		return "", struct{}{}, errors.New("the real fetch must not run in tests")
	}, nil)
	charge = flows.DefineActivity("ft.charge", func(context.Context, flows.Key, int) (string, string, error) {
		return "", "", errors.New("the real charge must not run in tests")
	}, func(context.Context, flows.Key, string) error {
		refunds.Add(1)
		return nil
	})

	_ = func() bool { flows.RegisterErrorKind("ft_declined", errDeclined); return true }()

	// weekly sleeps a week between two fetches.
	weekly = flows.DefineWorkflow("ft.weekly", 1, func(c flows.Ctx, url string) (string, error) {
		a, err := fetch.Run(c, url)
		if err != nil {
			return "", err
		}
		if err := flows.Sleep(c, 7*24*time.Hour); err != nil {
			return "", err
		}
		b, err := fetch.Run(c, url)
		return a + "," + b, err
	})
	approval = flows.DefineWorkflow("ft.approval", 1, func(c flows.Ctx, _ int) (bool, error) {
		return flows.Await[bool](c, "approved", 48*time.Hour)
	})
	stuck = flows.DefineWorkflow("ft.stuck", 1, func(c flows.Ctx, _ int) (int, error) {
		_, err := flows.Signals[int](c, "never").Receive(c)
		return 0, err
	})
	pay = flows.DefineWorkflow("ft.pay", 1, func(c flows.Ctx, amount int) (string, error) {
		if _, err := charge.Run(c, amount); err != nil {
			return "", err
		}
		return "", errDeclined
	})
)

func TestVirtualTime(t *testing.T) {
	tst := flowtest.New(t)
	calls := 0
	flowtest.OnActivity(tst, fetch, func(_ context.Context, _ flows.Key, url string) (string, struct{}, error) {
		calls++
		return url + "#" + strconv.Itoa(calls), struct{}{}, nil
	})
	start := tst.Now()
	began := time.Now()
	out, err := flowtest.Run(tst, weekly, "weekly", "u")
	if err != nil || out != "u#1,u#2" {
		t.Fatalf("%q %v", out, err)
	}
	if d := tst.Now().Sub(start); d < 7*24*time.Hour {
		t.Fatalf("virtual time moved %v", d)
	}
	if time.Since(began) > 5*time.Second {
		t.Fatalf("a week took %v of wall time", time.Since(began))
	}
}

func TestScheduledSignalAndTimeout(t *testing.T) {
	tst := flowtest.New(t)
	tst.SignalAfter(30*time.Hour, "approved", "approved", true)
	if ok, err := flowtest.Run(tst, approval, "approved", 0); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	// No signal: the 48h timeout fires.
	if _, err := flowtest.Run(tst, approval, "timeout", 0); !errors.Is(err, flows.ErrAwaitTimeout) {
		t.Fatalf("%v", err)
	}
}

// A workflow that can never end is reported at once, deterministically
// (go-workflows #481: a race picked wall-clock mode and the tester panicked
// after its timeout).
func TestGW17_481_BlockedIsReportedAtOnce(t *testing.T) {
	tst := flowtest.New(t)
	began := time.Now()
	if _, err := flowtest.Run(tst, stuck, "stuck", 0); !errors.Is(err, flowtest.ErrBlocked) {
		t.Fatalf("%v", err)
	}
	if time.Since(began) > 2*time.Second {
		t.Fatalf("took %v", time.Since(began))
	}
}

func TestCompensationWithMocks(t *testing.T) {
	tst := flowtest.New(t)
	refunds.Store(0)
	var refunded []string
	flowtest.OnActivity(tst, charge, func(_ context.Context, _ flows.Key, amount int) (string, string, error) {
		id := "ch_" + strconv.Itoa(amount)
		return id, id, nil
	})
	flowtest.OnCompensator(tst, charge, func(_ context.Context, _ flows.Key, id string) error {
		refunded = append(refunded, id)
		return nil
	})
	_, err := flowtest.Run(tst, pay, "pay", 10)
	if !errors.Is(err, errDeclined) || !errors.Is(err, flows.ErrCompensated) {
		t.Fatalf("%v", err)
	}
	if n := refunds.Load(); n != 0 {
		t.Fatalf("the real compensator ran %d times", n)
	}
	if len(refunded) != 1 || refunded[0] != "ch_10" {
		t.Fatalf("mocked compensator got %v", refunded)
	}
}

// GW-17, go-workflows #494: a timer canceled while an activity runs must
// not fire later (the tester tracked the wall-clock timer of the wrong
// timer and panicked).
var raceTimer = flows.DefineWorkflow("ft.race-timer", 1, func(c flows.Ctx, _ int) (string, error) {
	tc, cancel := flows.WithCancel(c)
	timer := flows.NewTimer(tc, 300*time.Millisecond)
	first := fetch.Start(c, "fast")
	won := ""
	flows.Select(c,
		flows.OnFuture(timer, func(flows.Ctx, struct{}, error) { won = "timer" }),
		flows.OnFuture(first, func(flows.Ctx, string, error) { won = "activity" }),
	)
	cancel()
	if err := flows.Sleep(c, time.Second); err != nil { // past the canceled timer
		return "", err
	}
	if _, err := fetch.Run(c, "slow"); err != nil {
		return "", err
	}
	if _, err := timer.Get(c); !errors.Is(err, flows.ErrCanceled) {
		return "", errors.New("canceled timer did not report ErrCanceled")
	}
	return won, nil
})

func TestGW17_494_CanceledTimerDoesNotFire(t *testing.T) {
	tst := flowtest.New(t)
	flowtest.OnActivity(tst, fetch, echo)
	if won, err := flowtest.Run(tst, raceTimer, "race", 0); err != nil || won != "activity" {
		t.Fatalf("%q %v", won, err)
	}
	// Two timers (the raced one, the sleep): one canceled, one fired.
	count := func(typ string) (n int) {
		for _, e := range tst.EventTypes("race") {
			if e == typ {
				n++
			}
		}
		return n
	}
	if count("TimerScheduled") != 2 || count("TimerCanceled") != 1 || count("TimerFired") != 1 {
		t.Fatalf("history %v", tst.EventTypes("race"))
	}
}

func echo(_ context.Context, _ flows.Key, url string) (string, struct{}, error) {
	return url, struct{}{}, nil
}

// GW-17, go-workflows #481: mocked activities that complete at once, timers
// and activities taking wall time never make the tester report a blocked
// workflow, and virtual time is the same on every run.
var mixed = flows.DefineWorkflow("ft.mixed", 1, func(c flows.Ctx, _ int) (time.Time, error) {
	for i := range 3 {
		if _, err := fetch.Run(c, strconv.Itoa(i)); err != nil {
			return time.Time{}, err
		}
		if err := flows.Sleep(c, time.Duration(i+1)*time.Minute); err != nil {
			return time.Time{}, err
		}
	}
	return flows.Now(c), nil
})

func TestGW17_481_NoSpuriousBlocked(t *testing.T) {
	var first time.Time
	for i := range 50 {
		tst := flowtest.New(t)
		flowtest.OnActivity(tst, fetch, func(_ context.Context, _ flows.Key, s string) (string, struct{}, error) {
			if s == "1" {
				time.Sleep(time.Millisecond) // wall time
			}
			return s, struct{}{}, nil
		})
		end, err := flowtest.Run(tst, mixed, "mixed", 0)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if i == 0 {
			first = end
		} else if !end.Equal(first) {
			t.Fatalf("run %d ended at %v, run 0 at %v", i, end, first)
		}
	}
}

// Work on a queue no engine of the tester runs is reported blocked at once.
var gpuJob = flows.DefineActivity("ft.gpu", func(context.Context, flows.Key, int) (int, struct{}, error) {
	return 1, struct{}{}, nil
}, nil, flows.WithQueue("gpu"))

var gpuWF = flows.DefineWorkflow("ft.gpu-wf", 1, func(c flows.Ctx, _ int) (int, error) { return gpuJob.Run(c, 0) })

func TestUnservedQueueIsBlocked(t *testing.T) {
	tst := flowtest.New(t, flowtest.WithOptions(flows.Options{DisableEager: true}))
	began := time.Now()
	if _, err := flowtest.Run(tst, gpuWF, "gpu", 0); !errors.Is(err, flowtest.ErrBlocked) {
		t.Fatalf("%v", err)
	}
	if time.Since(began) > 5*time.Second {
		t.Fatalf("took %v", time.Since(began))
	}
	served := flowtest.New(t, flowtest.WithOptions(flows.Options{DisableEager: true, Queues: []string{"default", "gpu"}}))
	if n, err := flowtest.Run(served, gpuWF, "gpu", 0); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
}

// Virtual time does not move while an activity runs (wall time).
func TestVirtualTimeStopsDuringActivities(t *testing.T) {
	tst := flowtest.New(t)
	start := tst.Now()
	flowtest.OnActivity(tst, fetch, func(_ context.Context, _ flows.Key, s string) (string, struct{}, error) {
		time.Sleep(50 * time.Millisecond)
		return s, struct{}{}, nil
	})
	if _, err := flowtest.Run(tst, weekly, "frozen", "u"); err != nil {
		t.Fatal(err)
	}
	if d := tst.Now().Sub(start); d != 7*24*time.Hour {
		t.Fatalf("virtual time moved %v, want exactly the sleep", d)
	}
}

// GW-17, go-workflows #436: a workflow that returns while an activity (or
// child) it started is still pending is reported.
var forgetful = flows.DefineWorkflow("ft.forgetful", 1, func(c flows.Ctx, _ int) (int, error) {
	fetch.Start(c, "slow") // never awaited
	if _, err := fetch.Run(c, "fast"); err != nil {
		return 0, err
	}
	return 1, nil
})

func slowFast(_ context.Context, _ flows.Key, url string) (string, struct{}, error) {
	if url == "slow" {
		time.Sleep(300 * time.Millisecond) // outlives the workflow
	}
	return url, struct{}{}, nil
}

var (
	kid        = flows.DefineWorkflow("ft.kid", 1, func(c flows.Ctx, _ int) (int, error) { return 0, flows.Sleep(c, time.Hour) })
	leavesAKid = flows.DefineWorkflow("ft.leaves-a-kid", 1, func(c flows.Ctx, _ int) (int, error) {
		kid.Child(c, "left-kid", 0)
		return 1, flows.Sleep(c, time.Minute)
	})
)

func TestGW17_436_PendingFuturesAreReported(t *testing.T) {
	inner := &recordingTB{TB: t}
	tst := flowtest.New(inner)
	flowtest.OnActivity(tst, fetch, slowFast)
	if v, err := flowtest.Run(tst, forgetful, "forgetful", 0); err != nil || v != 1 {
		t.Fatalf("%d %v", v, err)
	}
	if !inner.failed.Load() {
		t.Fatal("the pending activity was not reported")
	}
	ok := flowtest.New(t, flowtest.AllowPendingFutures())
	flowtest.OnActivity(ok, fetch, slowFast)
	if _, err := flowtest.Run(ok, forgetful, "allowed", 0); err != nil {
		t.Fatal(err)
	}
	kidTB := &recordingTB{TB: t}
	tk := flowtest.New(kidTB)
	if _, err := flowtest.Run(tk, leavesAKid, "leaves", 0); err != nil {
		t.Fatal(err)
	}
	if !kidTB.failed.Load() {
		t.Fatal("the pending child was not reported")
	}
}

// recordingTB records Errorf instead of failing the test.
type recordingTB struct {
	testing.TB
	failed atomic.Bool
	mu     sync.Mutex
	msgs   []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.failed.Store(true)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

// messages returns the reports, one per line.
func (r *recordingTB) messages() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.msgs, "\n")
}

// GW-17, go-workflows #377: fan-out over many coroutines stays fast (it
// was O(n^3) in the go-workflows tester).
var fanOut = flows.DefineWorkflow("ft.fan-out", 1, func(c flows.Ctx, n int) (int, error) {
	var wg flows.WaitGroup
	done := 0
	for i := range n {
		wg.Add(1)
		flows.Go(c, func(c flows.Ctx) {
			defer wg.Done()
			if _, err := fetch.Run(c, strconv.Itoa(i)); err == nil {
				done++
			}
		})
	}
	if err := wg.Wait(c); err != nil {
		return 0, err
	}
	return done, nil
})

func TestGW17_377_FanOutIsFast(t *testing.T) {
	tst := flowtest.New(t)
	flowtest.OnActivity(tst, fetch, echo)
	began := time.Now()
	if n, err := flowtest.Run(tst, fanOut, "fan-out", 256); err != nil || n != 256 {
		t.Fatalf("%d %v", n, err)
	}
	if d := time.Since(began); d > 10*time.Second {
		t.Fatalf("256 coroutines took %v", d)
	}
}

func BenchmarkFanOut(b *testing.B) {
	tst := flowtest.New(b)
	flowtest.OnActivity(tst, fetch, echo)
	for _, n := range []int{16, 64, 256} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			for i := range b.N {
				if _, err := flowtest.Run(tst, fanOut, "bench-"+strconv.Itoa(n)+"-"+strconv.Itoa(i), n); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// T9.2 review: what a workflow starts in its last task without awaiting it
// is not started (it leaves no event): flowtest still reports it.
var (
	dropsAtTheEnd = flows.DefineWorkflow("ft.drops-at-the-end", 1, func(c flows.Ctx, _ int) (int, error) {
		fetch.Start(c, "slow") // in the task that ends the workflow
		kid.Child(c, "dropped-kid", 0)
		return 1, nil
	})
)

func TestGW17_436_DroppedAtTheEndIsReported(t *testing.T) {
	inner := &recordingTB{TB: t}
	tst := flowtest.New(inner)
	flowtest.OnActivity(tst, fetch, slowFast)
	if v, err := flowtest.Run(tst, dropsAtTheEnd, "drops", 0); err != nil || v != 1 {
		t.Fatalf("%d %v", v, err)
	}
	if !inner.failed.Load() {
		t.Fatal("what the last task started was not reported")
	}
}

// T10.11: an activity waiting for the cancel a workflow timer would send is
// a documented limitation (virtual time does not move while activities
// run): Run fails with ErrNeverIdle, it does not hang. Mocking the activity
// to return makes the workflow end.
var (
	// stopWaiting ends waitsForCancel at the end of the test (sooner than
	// the tester's Stop deadline).
	stopWaiting    atomic.Pointer[chan struct{}]
	waitsForCancel = flows.DefineActivity("ft.waits-for-cancel", func(ctx context.Context, _ flows.Key, _ int) (int, struct{}, error) {
		select {
		case <-ctx.Done():
			return 0, struct{}{}, ctx.Err()
		case <-*stopWaiting.Load():
			return 0, struct{}{}, errors.New("test over")
		}
	}, nil)
	timerVsActivity = flows.DefineWorkflow("ft.timer-vs-activity", 1, func(c flows.Ctx, _ int) (string, error) {
		ac, cancel := flows.WithCancel(c)
		f := waitsForCancel.Start(ac, 0)
		won := ""
		flows.Select(c,
			flows.OnFuture(flows.NewTimer(c, time.Minute), func(flows.Ctx, struct{}, error) { cancel(); won = "timer" }),
			flows.OnFuture(f, func(flows.Ctx, int, error) { won = "activity" }),
		)
		return won, nil
	})
)

func TestActivityWaitingForATimerIsNeverIdle(t *testing.T) {
	flowtest.SetIdleTimeout(t, 300*time.Millisecond)
	stop := make(chan struct{})
	stopWaiting.Store(&stop)
	tst := flowtest.New(t, flowtest.AllowPendingFutures())
	t.Cleanup(func() { close(stop) }) // before the tester stops
	if _, err := flowtest.Run(tst, timerVsActivity, "race", 0); !errors.Is(err, flowtest.ErrNeverIdle) {
		t.Fatalf("got %v, want ErrNeverIdle", err)
	}
	mocked := flowtest.New(t)
	flowtest.OnActivity(mocked, waitsForCancel, func(context.Context, flows.Key, int) (int, struct{}, error) {
		return 0, struct{}{}, nil
	})
	if won, err := flowtest.Run(mocked, timerVsActivity, "race", 0); err != nil || won != "activity" {
		t.Fatalf("%q %v", won, err)
	}
}

// T10.11: the futures abandoned by the children (and grandchildren) of the
// instance passed to Run are reported too, both those left pending and
// those dropped at the end.
var (
	grandparent = flows.DefineWorkflow("ft.grandparent", 1, func(c flows.Ctx, _ int) (int, error) {
		return parentOfForgetful.Child(c, "parent-of-forgetful", 0).Get(c)
	})
	parentOfForgetful = flows.DefineWorkflow("ft.parent-of-forgetful", 1, func(c flows.Ctx, _ int) (int, error) {
		a, err := forgetful.Child(c, "forgetful-kid", 0).Get(c)
		if err != nil {
			return 0, err
		}
		b, err := dropsAtTheEnd.Child(c, "drops-kid", 0).Get(c)
		return a + b, err
	})
)

func TestChildrenPendingFuturesAreReported(t *testing.T) {
	inner := &recordingTB{TB: t}
	tst := flowtest.New(inner)
	flowtest.OnActivity(tst, fetch, slowFast)
	if v, err := flowtest.Run(tst, grandparent, "grandparent", 0); err != nil || v != 2 {
		t.Fatalf("%d %v", v, err)
	}
	msgs := inner.messages()
	for _, want := range []string{"forgetful-kid", "drops-kid"} {
		if !strings.Contains(msgs, want) {
			t.Errorf("%s not reported: %q", want, msgs)
		}
	}
	if strings.Contains(msgs, "parent-of-forgetful ended") || strings.Contains(msgs, "grandparent ended") {
		t.Errorf("an instance that awaited everything was reported: %q", msgs)
	}
	ok := flowtest.New(t, flowtest.AllowPendingFutures())
	flowtest.OnActivity(ok, fetch, slowFast)
	if _, err := flowtest.Run(ok, grandparent, "grandparent", 0); err != nil {
		t.Fatal(err)
	}
}
