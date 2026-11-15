package p

import (
	"context"
	cryptorand "crypto/rand"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"time"

	fl "github.com/imdat99/go-flows"
)

var order = fl.DefineWorkflow("order", 1, func(c fl.Ctx, items map[string]int) (int, error) {
	for range items { // want "iterating over a map is not deterministic"
	}
	keys := make([]string, 0, len(items))
	for k := range keys { // a slice: fine
		_ = k
	}
	sort.Strings(keys)
	_ = time.Now()            // want `time.Now is not deterministic in workflow code: use flows.Now`
	time.Sleep(time.Second)   // want `time.Sleep is not deterministic in workflow code: use flows.Sleep`
	_ = time.Duration(5)      // a conversion: fine
	_ = rand.IntN(3)          // want `math/rand/v2.IntN is not deterministic`
	_, _ = cryptorand.Read(nil) // want `crypto/rand.Read is not deterministic`
	_ = os.Getenv("X")        // want `os.Getenv is not deterministic in workflow code`
	go func() {}()            // want "use flows.Go instead of a go statement"
	ch := make(chan int, 1)   // want "native channels are not allowed"
	ch <- 1                   // want "native channels are not allowed"
	<-ch                      // want "native channels are not allowed"
	select {}                 // want "select is not deterministic"
	var wg sync.WaitGroup     // want "sync.WaitGroup blocks workflow code"
	_ = wg
	var mu sync.Mutex // want "sync.Mutex is not needed"
	_ = mu
	var ok fl.WaitGroup // fine
	_ = ok
	// The value of a side effect is recorded: anything goes in it.
	id, _ := fl.SideEffect(c, func() int64 { return time.Now().UnixNano() + rand.Int64() })
	_ = id
	// A coroutine is workflow code too.
	fl.Go(c, func(c fl.Ctx) {
		_ = time.Now() // want `time.Now is not deterministic`
	})
	return 0, nil
})

// helper takes a Ctx: workflow code.
func helper(c fl.Ctx) time.Time {
	return time.Now() // want `time.Now is not deterministic`
}

// Activities may do anything.
func init() {
	fl.DefineActivity("charge", func(ctx context.Context, k fl.Key, in int) (int, int, error) {
		time.Sleep(time.Millisecond)
		go func() {}()
		_ = time.Now()
		return in, in, nil
	})
}

// Ordinary code is not checked.
func plain() {
	_ = time.Now()
	go func() {}()
}
