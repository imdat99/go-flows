package s

import (
	"sync"
	"time"

	fl "github.com/imdat99/go-flows/flows"
)

type state struct {
	ch chan int
	mu sync.Mutex
	n  int
}

type nested struct {
	st  state
	ptr *sync.Mutex // behind a pointer: its method calls are reported
}

type embeds struct {
	sync.RWMutex
}

type arrays struct {
	wgs [2]sync.WaitGroup
}

type plain struct {
	n int
	d time.Duration
}

func consume(chan int) {}

func consumeAny(any) {}

// T10.12: channels not bound to a variable, struct fields of channel or
// sync types, and side-effect functions held in a variable.
var wf = fl.DefineWorkflow("t1012", 1, func(c fl.Ctx, in int) (int, error) {
	consume(make(chan int))       // want "native channels are not allowed"
	consumeAny(make(chan int, 1)) // want "native channels are not allowed"
	_ = make(chan struct{})       // want "native channels are not allowed"
	ch := make(chan int)          // want "native channels are not allowed"
	var ch2 = make(chan int)      // want "native channels are not allowed"
	_, _ = ch, ch2
	_ = make([]int, 3) // not a channel: fine

	var st state      // want `native channels are not allowed in workflow code \(field ch\)` `sync.Mutex \(field mu\) is not needed`
	p := &state{}     // want `\(field ch\)` `\(field mu\)`
	var n nested      // want `\(field st.ch\)` `\(field st.mu\)`
	var e embeds      // want `sync.RWMutex \(field RWMutex\) is not needed`
	var a arrays      // want `sync.WaitGroup \(field wgs\) blocks workflow code`
	var anon struct { // want `\(field done\)`
		done chan bool
	}
	var ok plain        // fine
	var wg fl.WaitGroup // fine
	_, _, _, _, _, _, _, _ = st, p, n, e, a, anon, ok, wg

	// A side-effect function held in a variable is not workflow code.
	now := func() int64 { return time.Now().UnixNano() }
	_, _ = fl.SideEffect(c, now)
	var then func() int64 = func() int64 { return time.Now().Unix() }
	_, _ = fl.SideEffect(c, then)
	later := func() int64 { return 0 }
	later = func() int64 { return time.Now().Unix() }
	_, _ = fl.SideEffect[int64](c, later)

	// Also called in workflow code: checked.
	both := func() int64 { return time.Now().Unix() } // want `time.Now is not deterministic`
	_, _ = fl.SideEffect(c, both)
	_ = both()
	// Passed to something else too: checked.
	shared := func() int64 { return time.Now().Unix() } // want `time.Now is not deterministic`
	_, _ = fl.SideEffect(c, shared)
	keep(shared)
	// A package variable: its other uses are not all seen, checked.
	global = func() int64 { return time.Now().Unix() } // want `time.Now is not deterministic`
	_, _ = fl.SideEffect(c, global)
	return 0, nil
})

var global func() int64

func keep(func() int64) {}
