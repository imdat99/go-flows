package r

import (
	"context"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	fl "github.com/imdat99/go-flows"
)

type state struct {
	wg sync.WaitGroup
	mu sync.Mutex
}

// T8.4 review: non-deterministic calls that were not reported.
var wf = fl.DefineWorkflow("t84", 1, func(c fl.Ctx, s *state) (int, error) {
	_ = uuid.New()                               // want `github.com/google/uuid.New is not deterministic`
	_ = uuid.NewString()                         // want `github.com/google/uuid.NewString is not deterministic`
	_, _ = uuid.NewV7()                          // want `github.com/google/uuid.NewV7 is not deterministic`
	_ = uuid.NewSHA1(uuid.UUID{}, nil)           // derived from its input: fine
	_ = uuid.MustParse("x")                      // fine
	_, _ = os.Hostname()                         // want `os.Hostname is not deterministic`
	_ = os.Environ()                             // want `os.Environ is not deterministic`
	_ = os.Getpid()                              // want `os.Getpid is not deterministic`
	_, _ = context.WithTimeout(nil, time.Second) // want `context.WithTimeout is not deterministic`
	s.wg.Go(func() {})                           // want `sync.WaitGroup.Go`
	s.mu.Lock()                                  // want `sync.Mutex.Lock`
	var g errgroup.Group
	g.Go(func() error { return nil }) // want `errgroup.Group.Go starts a goroutine`
	src := rand.NewPCG(1, 2)          // a constructor: fine
	_ = rand.New(src).IntN(3)         // want `math/rand/v2.Rand.IntN is not deterministic`
	return 0, nil
})
