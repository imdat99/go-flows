package testdb

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Proxy is an in-process TCP proxy in front of the test server. Freeze
// makes it a blackhole (a failover, a partition without RST): it stops
// forwarding in both directions without closing anything, so peers wait
// on reads that never return. Thaw resumes forwarding (what was held is
// delivered).
type Proxy struct {
	l          net.Listener
	targetHost string
	targetPort uint16

	mu     sync.Mutex
	thawed chan struct{} // closed while forwarding; replaced by Freeze
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func newProxy(tb testing.TB, host string, port uint16) *Proxy {
	tb.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("testdb: proxy listen: %v", err)
	}
	p := &Proxy{l: l, targetHost: host, targetPort: port, thawed: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	close(p.thawed)
	p.wg.Go(p.accept)
	return p
}

// Port is the proxy's listening port (on 127.0.0.1).
func (p *Proxy) Port() uint16 {
	a, _ := p.l.Addr().(*net.TCPAddr) // a TCP listener
	return uint16(a.Port)
}

// DirectConfig returns a copy of cfg (a proxied pool's) that connects to
// the server directly, bypassing the proxy: another engine on the same
// database that a freeze does not affect.
func (p *Proxy) DirectConfig(cfg *pgxpool.Config) *pgxpool.Config {
	c := cfg.Copy()
	c.ConnConfig.Host, c.ConnConfig.Port = p.targetHost, p.targetPort
	return c
}

// Freeze stops forwarding (idempotent).
func (p *Proxy) Freeze() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.thawed:
		p.thawed = make(chan struct{})
	default:
	}
}

// Thaw resumes forwarding (idempotent).
func (p *Proxy) Thaw() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.thawed:
	default:
		close(p.thawed)
	}
}

// gate returns once forwarding is allowed; false when the proxy closed.
func (p *Proxy) gate() bool {
	p.mu.Lock()
	ch := p.thawed
	p.mu.Unlock()
	<-ch // Close thaws
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.closed
}

// Close thaws the proxy, then closes the listener and every connection.
func (p *Proxy) Close() {
	p.Thaw()
	p.mu.Lock()
	p.closed = true
	conns := p.conns
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	_ = p.l.Close()
	for c := range conns {
		_ = c.Close()
	}
	p.wg.Wait()
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(cs ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range cs {
		delete(p.conns, c)
	}
}

func (p *Proxy) accept() {
	target := net.JoinHostPort(p.targetHost, strconv.Itoa(int(p.targetPort)))
	for {
		c, err := p.l.Accept()
		if err != nil {
			return
		}
		p.wg.Go(func() {
			if !p.gate() || !p.track(c) { // a frozen proxy does not dial either
				_ = c.Close()
				return
			}
			var d net.Dialer
			u, err := d.DialContext(context.Background(), "tcp", target)
			if err != nil || !p.track(u) {
				_ = c.Close()
				if u != nil {
					_ = u.Close()
				}
				p.untrack(c, u)
				return
			}
			p.wg.Go(func() { p.pipe(u, c) })
			p.pipe(c, u)
		})
	}
}

// pipe copies src to dst, holding what it read while frozen; it closes
// both ends when either fails (the other direction's pipe then ends too).
func (p *Proxy) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.gate() {
				break
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = dst.Close()
	_ = src.Close()
	p.untrack(dst, src)
}

// NewProxied returns a pool as New does, whose connections go through a
// Proxy the test can freeze; configure, if any, adjusts the pool's
// configuration. When the test ends the proxy is thawed first, so that the
// pool closes cleanly.
func NewProxied(tb testing.TB, configure ...func(*pgxpool.Config)) (*pgxpool.Pool, *Proxy) {
	tb.Helper()
	once.Do(start)
	if errInit != nil {
		tb.Fatalf("testdb: %v", errInit)
	}
	cfg, err := pgxpool.ParseConfig(baseDSN)
	if err != nil {
		tb.Fatalf("testdb: parse dsn: %v", err)
	}
	p := newProxy(tb, cfg.ConnConfig.Host, cfg.ConnConfig.Port)
	tb.Cleanup(p.Close) // runs last: after the pool's cleanup below
	name := createDB(tb)
	cfg.ConnConfig.Database = name
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.Port = p.Port()
	cfg.ConnConfig.Fallbacks = nil
	for _, f := range configure {
		f(cfg)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		dropDB(name)
		tb.Fatalf("testdb: pool: %v", err)
	}
	tb.Cleanup(func() {
		p.Thaw()
		pool.Close()
		dropDB(name)
	})
	return pool, p
}
