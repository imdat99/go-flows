package q

import (
	"maps"
	mrand "math/rand"
	"os"
	"slices"
	"sync"
	"time"

	fl "github.com/imdat99/go-flows/flows"
)

var src = mrand.New(mrand.NewSource(1)) // outside workflow code: fine

type Ctx = fl.Ctx // an alias

type Signals chan int // a named channel type

type M map[string]int

func aliasCtx(c Ctx) {
	_ = time.Now() // want `time.Now is not deterministic`
}

func generic[T any](c fl.Ctx, v T) T {
	time.Sleep(time.Second) // want `time.Sleep is not deterministic`
	return v
}

func genericMap[K comparable, V any, MM ~map[K]V](c fl.Ctx, m MM) {
	for range m { // want "iterating over a map is not deterministic"
	}
}

type svc struct{}

func (svc) step(c fl.Ctx) {
	_ = mrand.Intn(3) // want `math/rand.Intn is not deterministic`
}

func notFirst(x int, c fl.Ctx) { // a Ctx not first: not workflow code
	_ = time.Now()
}

func rules(c fl.Ctx, m M, sm *sync.Map) {
	for range m { // want "iterating over a map is not deterministic"
	}
	for range maps.Keys(m) { // want "iterating over a map is not deterministic"
	}
	for range maps.All(m) { // want "iterating over a map is not deterministic"
	}
	for _, k := range slices.Sorted(maps.Keys(m)) { // sorted: fine
		_ = k
	}
	var s Signals // want "native channels are not allowed"
	for range s { // want "native channels are not allowed"
	}
	_ = src.Intn(3) // want `math/rand.Rand.Intn is not deterministic`
	sm.Range(func(k, v any) bool { return true }) // want `sync.Map.Range iterates in no fixed order`
	now := time.Now // want `time.Now is not deterministic`
	_ = now
	call(time.Since) // want `time.Since is not deterministic`
	_ = time.NewTicker(time.Second) // want `time.NewTicker is not deterministic`
	_ = time.Tick(time.Second)      // want `time.Tick is not deterministic`
	time.AfterFunc(0, func() {})    // want `time.AfterFunc is not deterministic`
	_, _ = os.LookupEnv("X")        // want `os.LookupEnv is not deterministic`
	var rw sync.RWMutex // want "sync.RWMutex is not needed"
	_ = &rw
	var once sync.Once // want "sync.Once is not needed"
	_ = &once
	mu := &sync.Mutex{} // want "sync.Mutex is not needed"
	_ = mu
	defer time.Sleep(0) // want `time.Sleep is not deterministic`
	fl.Go(c, func(c fl.Ctx) {
		select {} // want "select is not deterministic"
	})
	f := func() {
		_ = time.Now() // want `time.Now is not deterministic`
	}
	f()
	// Side effects: the recorded function is free; the Ctx argument is not.
	_, _ = fl.SideEffect[int64](c, func() int64 {
		g := func() int64 { return time.Now().Unix() }
		go func() {}()
		return g()
	})
	_, _ = fl.SideEffect(ctxAt(c, time.Now()), func() int { return 0 }) // want `time.Now is not deterministic`
}

func call(func(time.Time) time.Duration) {}

func ctxAt(c fl.Ctx, _ time.Time) fl.Ctx { return c }
