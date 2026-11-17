package flows_test

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
)

// freezeProxy forwards TCP connections to a Postgres server; frozen, it
// stops forwarding without closing anything, like a network that
// blackholes (failover, a drop without RST).
type freezeProxy struct {
	ln     net.Listener
	target string
	frozen atomic.Bool

	mu     sync.Mutex
	conns  []net.Conn
	closed bool
	wg     sync.WaitGroup
}

func newFreezeProxy(t *testing.T, target string) *freezeProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &freezeProxy{ln: ln, target: target}
	p.wg.Go(func() { p.accept(t.Context()) })
	t.Cleanup(p.close)
	return p
}

func (p *freezeProxy) accept(ctx context.Context) {
	var d net.Dialer
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		u, err := d.DialContext(ctx, "tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = c.Close()
			_ = u.Close()
			return
		}
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		p.wg.Go(func() { p.pipe(u, c) })
		p.wg.Go(func() { p.pipe(c, u) })
	}
}

// pipe copies src to dst, holding what it read while frozen.
func (p *freezeProxy) pipe(dst, src net.Conn) {
	defer func() { _ = dst.Close() }()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for p.frozen.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				_ = src.Close()
			}
			return
		}
	}
}

// close thaws and closes every connection.
func (p *freezeProxy) close() {
	p.frozen.Store(false)
	_ = p.ln.Close()
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// T11.1 on Postgres: when the database blackholes under an engine, the
// activity it runs is canceled with ErrLeaseLost once its lease can no
// longer be trusted, before another engine claims the expired lease and
// runs the next attempt, so the attempts do not overlap.
func TestBlackholedDatabaseGivesUpLeases(t *testing.T) {
	const ttl = 6 * time.Second // margin ttl/3 = 2s between giving up and the lease expiring
	pool := testdb.New(t)
	ctx := context.Background()
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	cc := pool.Config().ConnConfig
	proxy := newFreezeProxy(t, net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))))
	cfg := pool.Config().Copy()
	addr, ok := proxy.ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("proxy address %v", proxy.ln.Addr())
	}
	cfg.ConnConfig.Host = addr.IP.String()
	cfg.ConnConfig.Port = uint16(addr.Port)
	cfg.ConnConfig.Fallbacks = nil
	viaProxy, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(viaProxy.Close)

	type attempt struct {
		start, end time.Time
		cause      error
	}
	var (
		mu       sync.Mutex
		attempts []attempt
	)
	first := make(chan struct{})
	act := flows.DefineActivity(testdb.UniqueName(t, "act"), func(actx context.Context, _ flows.Key, in int) (int, struct{}, error) {
		mu.Lock()
		n := len(attempts)
		attempts = append(attempts, attempt{start: time.Now()})
		mu.Unlock()
		if n > 0 {
			return in, struct{}{}, nil // a later attempt: done at once
		}
		close(first)
		<-actx.Done()
		mu.Lock()
		attempts[0].end, attempts[0].cause = time.Now(), context.Cause(actx)
		mu.Unlock()
		return 0, struct{}{}, actx.Err()
	}, nil)
	wf := flows.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c flows.Ctx, in int) (int, error) {
		return act.Run(c, in)
	})

	start := func(p *pgxpool.Pool, id string) *flows.Engine {
		e, err := flows.New(p, flows.Options{InstanceID: id, LeaseTTL: ttl, PollMax: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			proxy.frozen.Store(false) // let A stop
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			_ = e.Stop(sctx)
		})
		return e
	}
	a := start(viaProxy, "a")
	if _, err := wf.Start(ctx, a, "i1", 7); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
	case <-time.After(30 * time.Second):
		t.Fatal("the first attempt never started")
	}
	proxy.frozen.Store(true)
	start(pool, "b")

	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	client, err := flows.New(pool, flows.Options{InstanceID: "client"}) // not started: client only
	if err != nil {
		t.Fatal(err)
	}
	out, err := wf.Handle(client, "i1").Wait(wctx)
	if err != nil || out != 7 {
		t.Fatalf("result %d %v", out, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) < 2 {
		t.Fatalf("%d attempts", len(attempts))
	}
	first0, second := attempts[0], attempts[1]
	if first0.end.IsZero() {
		t.Fatal("the first attempt was never canceled")
	}
	if !errors.Is(first0.cause, flows.ErrLeaseLost) {
		t.Fatalf("first attempt canceled by %v, want ErrLeaseLost", first0.cause)
	}
	if !first0.end.Before(second.start) {
		t.Fatalf("the attempts overlapped: first canceled at +%v, second started at +%v",
			first0.end.Sub(first0.start), second.start.Sub(first0.start))
	}
}
