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
	l      net.Listener
	target string

	mu     sync.Mutex
	thawed chan struct{} // closed while forwarding; replaced by Freeze
	conns  []net.Conn
	closed bool
	wg     sync.WaitGroup
}

func newProxy(tb testing.TB, target string) *Proxy {
	tb.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("testdb: proxy listen: %v", err)
	}
	p := &Proxy{l: l, target: target, thawed: make(chan struct{})}
	close(p.thawed)
	p.wg.Go(p.accept)
	return p
}

// Port is the proxy's listening port (on 127.0.0.1).
func (p *Proxy) Port() uint16 {
	a, _ := p.l.Addr().(*net.TCPAddr) // a TCP listener
	return uint16(a.Port)
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
	p.conns = nil
	p.mu.Unlock()
	_ = p.l.Close()
	for _, c := range conns {
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
	p.conns = append(p.conns, c)
	return true
}

func (p *Proxy) accept() {
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
			u, err := d.DialContext(context.Background(), "tcp", p.target)
			if err != nil || !p.track(u) {
				_ = c.Close()
				if u != nil {
					_ = u.Close()
				}
				return
			}
			p.wg.Go(func() { p.pipe(u, c) })
			p.pipe(c, u)
		})
	}
}

// pipe copies src to dst, holding what it read while frozen.
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
	target := net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port)))
	p := newProxy(tb, target)
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
		p.Close()
		tb.Fatalf("testdb: pool: %v", err)
	}
	tb.Cleanup(func() {
		p.Thaw()
		pool.Close()
		p.Close()
		dropDB(name)
	})
	return pool, p
}
