//go:build chaos

package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/history"
	"github.com/imdat99/go-flows/internal/testdb"
)

// cluster is a set of child processes ("soak" mode) running engines on one
// database, each through its own proxy (slot), and the parent's side:
// a client engine starting the workload, a check pool the faults never
// touch (application_name chaos-check, no statement_timeout), and the
// record of the process faults (exits, SIGSTOP windows) the overlap check
// needs.
type cluster struct {
	t      *testing.T
	check  *pgxpool.Pool
	client *flows.Engine
	dbName string
	direct string // DSN of the database, not proxied
	ttl    time.Duration
	env    []string // extra environment of the children
	slots  []*slot

	mu     sync.Mutex
	gen    int
	exits  map[int]time.Time // pid -> exit
	stops  map[int][]window  // pid -> SIGSTOP windows
	holes  map[int][]window  // pid -> blackholes of its connections (diagnostics)
	ids    []int             // started orders
	nextID int
	faults map[string]int
	starts sync.WaitGroup // starting orders (Start, then the signal)
}

type window struct{ from, to time.Time } // to zero: open

type slot struct {
	idx   int
	proxy *testdb.Proxy
	dsn   string
	proc  *soakProc
}

type soakProc struct {
	name   string
	cmd    *exec.Cmd
	out    *tailBuffer
	exited chan struct{}
}

func (p *soakProc) pid() int { return p.cmd.Process.Pid }

// tailBuffer keeps the last bytes a child wrote, and counts the writes
// mentioning a statement timeout (SQLSTATE 57014).
type tailBuffer struct {
	mu       sync.Mutex
	buf      []byte
	timeouts map[string]int // by log message
}

const tailMax = 64 << 10

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > 2*tailMax {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-tailMax:]...)
	}
	if s := string(p); strings.Contains(s, "57014") || strings.Contains(s, "statement timeout") {
		if b.timeouts == nil {
			b.timeouts = make(map[string]int)
		}
		b.timeouts[logMsg(s)]++
	}
	return len(p), nil
}

// logMsg returns the message of a log line of slog's text handler.
func logMsg(line string) string {
	_, msg, ok := strings.Cut(line, "msg=")
	if !ok {
		return "?"
	}
	if q, ok := strings.CutPrefix(msg, `"`); ok {
		msg, _, _ = strings.Cut(q, `"`)
		return msg
	}
	msg, _, _ = strings.Cut(msg, " ")
	return msg
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// addTimeouts adds the statement timeouts logged, by message, to m.
func (b *tailBuffer) addTimeouts(m map[string]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for k, v := range b.timeouts {
		m[k] += v
	}
}

func newCluster(t *testing.T, n int, ttl time.Duration, env ...string) *cluster {
	t.Helper()
	pool := setup(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, soakSchema); err != nil {
		t.Fatal(err)
	}
	cl := &cluster{
		t: t, dbName: pool.Config().ConnConfig.Database, direct: testdb.DSN(pool), ttl: ttl, env: env,
		exits: make(map[int]time.Time), stops: make(map[int][]window), holes: make(map[int][]window), faults: make(map[string]int),
	}
	cl.check = cl.pool("chaos-check", true)
	db.Store(cl.check)                                                                             // dumps and waitFor: never cut
	cl.client = mustEngine(t, cl.pool("chaos-client", false), flows.Options{InstanceID: "client"}) // not started
	t.Cleanup(func() {
		if t.Failed() {
			cl.dump()
		}
	})
	for i := range n {
		proxy, dsn := testdb.NewProxy(t, pool)
		s := &slot{idx: i, proxy: proxy, dsn: dsn}
		cl.slots = append(cl.slots, s)
		cl.spawn(s)
	}
	for _, s := range cl.slots {
		cl.waitReady(s.proc)
	}
	return cl
}

// pool returns a new pool on the database; check pools are never cut and
// have no statement_timeout.
func (cl *cluster) pool(app string, check bool) *pgxpool.Pool {
	cl.t.Helper()
	cfg, err := pgxpool.ParseConfig(cl.direct)
	if err != nil {
		cl.t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	if check {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "0"
	}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		cl.t.Fatal(err)
	}
	cl.t.Cleanup(p.Close)
	return p
}

func mustEngine(t *testing.T, pool *pgxpool.Pool, opts flows.Options) *flows.Engine {
	t.Helper()
	e, err := flows.New(pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// spawn starts a new child process in slot s.
func (cl *cluster) spawn(s *slot) {
	cl.t.Helper()
	cl.mu.Lock()
	cl.gen++
	name := "s" + strconv.Itoa(s.idx) + "-g" + strconv.Itoa(cl.gen)
	cl.mu.Unlock()
	cmd := exec.CommandContext(cl.t.Context(), os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), envMode+"=soak", envDSN+"="+s.dsn, envAppDSN+"="+cl.direct,
		envTTL+"="+cl.ttl.String(), envInstance+"="+name)
	cmd.Env = append(cmd.Env, cl.env...)
	out := &tailBuffer{}
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		cl.t.Fatal(err)
	}
	p := &soakProc{name: name, cmd: cmd, out: out, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		cl.mu.Lock()
		now := time.Now()
		cl.exits[cmd.Process.Pid] = now
		if ws := cl.stops[cmd.Process.Pid]; len(ws) > 0 && ws[len(ws)-1].to.IsZero() {
			ws[len(ws)-1].to = now
		}
		cl.mu.Unlock()
		close(p.exited)
	}()
	cl.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.exited
		if cl.t.Failed() {
			cl.t.Logf("child %s (pid %d) output (tail):\n%s", name, cmd.Process.Pid, out.String())
		}
	})
	s.proc = p
}

func (cl *cluster) waitReady(p *soakProc) {
	cl.t.Helper()
	waitFor(cl.t, "child "+p.name+" to start", time.Minute, func() bool {
		select {
		case <-p.exited:
			cl.t.Fatalf("child %s exited:\n%s", p.name, p.out.String())
		default:
		}
		return strings.Contains(p.out.String(), "READY")
	})
}

func (cl *cluster) count(fault string) {
	cl.mu.Lock()
	cl.faults[fault]++
	cl.mu.Unlock()
}

// ---- faults ----

// kill kills the process of s, then starts another in s.
func (cl *cluster) kill(s *slot) {
	cl.t.Helper()
	_ = s.proc.cmd.Process.Signal(syscall.SIGKILL)
	<-s.proc.exited
	cl.count("kill")
	cl.spawn(s)
}

// pause freezes the process of s (SIGSTOP) for d.
func (cl *cluster) pause(s *slot, d time.Duration) {
	pid := s.proc.pid()
	cl.mu.Lock()
	if err := s.proc.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		cl.mu.Unlock()
		return
	}
	cl.stops[pid] = append(cl.stops[pid], window{from: time.Now()})
	cl.mu.Unlock()
	time.Sleep(d)
	cl.mu.Lock()
	_ = s.proc.cmd.Process.Signal(syscall.SIGCONT)
	if ws := cl.stops[pid]; ws[len(ws)-1].to.IsZero() {
		ws[len(ws)-1].to = time.Now()
	}
	cl.mu.Unlock()
	cl.count("sigstop")
}

// blackhole freezes the connections of s (the database stops answering
// it) for d.
func (cl *cluster) blackhole(s *slot, d time.Duration) {
	from := time.Now()
	s.proxy.Freeze()
	time.Sleep(d)
	s.proxy.Thaw()
	cl.hole(s.proc.pid(), from)
	cl.count("blackhole")
}

// hole records a blackhole of pid's connections, begun at from, ended now.
func (cl *cluster) hole(pid int, from time.Time) {
	cl.mu.Lock()
	cl.holes[pid] = append(cl.holes[pid], window{from: from, to: time.Now()})
	cl.mu.Unlock()
}

// frozenStop freezes the connections of s, waits after, then stops its
// engine (SIGTERM: Stop with soakStopDeadline) and checks that Stop
// returns within its deadline (T11.2) and the process exits; then thaws
// and starts another process in s. It returns how long Stop took.
func (cl *cluster) frozenStop(s *slot, after time.Duration) time.Duration {
	cl.t.Helper()
	from := time.Now()
	s.proxy.Freeze()
	defer s.proxy.Thaw()
	defer cl.hole(s.proc.pid(), from)
	time.Sleep(after)
	p := s.proc
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		cl.t.Fatal(err)
	}
	t0 := time.Now()
	var took time.Duration
	// T11.2: Stop returns by the deadline + stopGrace (1s) + unstickWait
	// (250ms); 2s more for -race on a loaded machine.
	const slack = 1250*time.Millisecond + 2*time.Second
	waitFor(cl.t, "the frozen engine's Stop", soakStopDeadline+slack+10*time.Second, func() bool {
		o := p.out.String()
		i := strings.LastIndex(o, "STOPPED ")
		if i < 0 {
			select {
			case <-p.exited:
				cl.t.Fatalf("child %s exited without stopping:\n%s", p.name, o)
			default:
			}
			return false
		}
		f := strings.Fields(o[i+len("STOPPED "):])
		ms, err := strconv.Atoi(f[0])
		if err != nil {
			cl.t.Fatalf("STOPPED line: %q", o[i:])
		}
		took = time.Duration(ms) * time.Millisecond
		return true
	})
	if took > soakStopDeadline+slack {
		cl.t.Errorf("Stop of a frozen engine took %v (deadline %v)", took, soakStopDeadline)
	}
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		cl.t.Errorf("child %s did not exit %v after SIGTERM", p.name, time.Since(t0))
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
	s.proxy.Thaw()
	cl.count("frozen-stop")
	cl.spawn(s)
	return took
}

// terminate terminates every backend of the database but the check
// pool's (pg_terminate_backend) and returns how many.
func (cl *cluster) terminate() int {
	var n int
	if err := cl.check.QueryRow(context.Background(), `
		SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid() AND application_name <> 'chaos-check'`, cl.dbName).Scan(&n); err != nil {
		cl.t.Errorf("terminate: %v", err)
	}
	cl.count("terminate")
	return n
}

// setStatementTimeout sets (d > 0) or resets the database's default
// statement_timeout, as a DBA would, and terminates the backends so that
// the pools reconnect with it.
func (cl *cluster) setStatementTimeout(d time.Duration) {
	q := `ALTER DATABASE ` + cl.dbName + ` RESET statement_timeout`
	if d > 0 {
		q = `ALTER DATABASE ` + cl.dbName + ` SET statement_timeout = '` + strconv.Itoa(int(d.Milliseconds())) + `ms'`
		cl.count("statement-timeout")
	}
	if _, err := cl.check.Exec(context.Background(), q); err != nil {
		cl.t.Errorf("%s: %v", q, err)
	}
	cl.terminate()
}

// ---- workload ----

// startOrders starts n orders (in goroutines: the starts retry through
// faults; wait with startsDone).
func (cl *cluster) startOrders(n int) {
	cl.mu.Lock()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = cl.nextID
		cl.nextID++
	}
	cl.ids = append(cl.ids, ids...)
	cl.mu.Unlock()
	for _, id := range ids {
		cl.starts.Go(func() { cl.startOrder(id) })
	}
}

// startOrder starts order id and signals it, retrying through faults
// (Start is idempotent; a signal sent twice is harmless: only one is read).
func (cl *cluster) startOrder(id int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	retry := func(what string, f func() error) bool {
		for {
			err := f()
			if err == nil {
				return true
			}
			if ctx.Err() != nil {
				cl.t.Errorf("%s %s: %v", what, soakID(id), err)
				return false
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !retry("start", func() error {
		_, err := soakOrder.Start(ctx, cl.client, soakID(id), treeIn{ID: id})
		return err
	}) || !soakSignaled(id) {
		return
	}
	retry("signal", func() error { return cl.client.Signal(ctx, soakID(id), "bump", soakBump(id)) })
}

func (cl *cluster) startsDone() {
	cl.t.Helper()
	done := make(chan struct{})
	go func() { cl.starts.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(6 * time.Minute):
		cl.t.Fatal("orders not started")
	}
}

// open is the number of orders started and not ended.
func (cl *cluster) open() int {
	cl.t.Helper()
	n, err := cl.openN()
	if err != nil {
		cl.t.Fatal(err)
	}
	return n
}

// openN is open, for any goroutine.
func (cl *cluster) openN() (int, error) {
	cl.mu.Lock()
	n := len(cl.ids)
	cl.mu.Unlock()
	var ended int
	err := cl.check.QueryRow(context.Background(), `
		SELECT count(*) FROM flows.execution
		WHERE workflow = 'soak.order' AND status NOT IN ('running', 'compensating', 'needs_attention', 'continued')`).Scan(&ended)
	return n - ended, err
}

// parked fails the test at once on an execution parked as needs_attention.
func (cl *cluster) parked() {
	cl.t.Helper()
	if n := count(cl.t, cl.check, `SELECT count(*) FROM flows.execution WHERE status = 'needs_attention'`); n > 0 {
		dumpQuery(cl.t, cl.check, "parked (id instance err kinds)",
			`SELECT id, instance_id, err, err_kinds FROM flows.execution WHERE status = 'needs_attention' ORDER BY id`)
		cl.t.Fatalf("%d executions parked as needs_attention", n)
	}
}

// waitEnded waits for every order started to end and for nothing to run.
func (cl *cluster) waitEnded(timeout time.Duration) {
	cl.t.Helper()
	cl.startsDone()
	waitFor(cl.t, "every execution to end", timeout, func() bool {
		cl.parked()
		return cl.open() == 0 &&
			count(cl.t, cl.check, `SELECT count(*) FROM flows.execution WHERE status IN ('running', 'compensating')`) == 0
	})
}

// ---- integrity ----

// verify checks the end state: every execution ended as it should (none
// parked), outputs, no task, lock or waiter left, histories recording each
// outcome once, effects once per key, and no overlapping attempts.
func (cl *cluster) verify() {
	cl.t.Helper()
	t := cl.t
	cl.mu.Lock()
	ids := slices.Clone(cl.ids)
	cl.mu.Unlock()
	cl.verifyExecutions(ids)
	for _, q := range []struct{ what, query string }{
		{"activity tasks left", `SELECT count(*) FROM flows.activity_task`},
		{"lock rows left", `SELECT count(*) FROM flows.lock`},
		{"lock waiters left", `SELECT count(*) FROM flows.lock_waiter`},
		// Not expected with the workload's retry policy (T11.3: lost attempts
		// count toward it).
		{"activities failed with their attempts exhausted", `SELECT count(*) FROM flows.history WHERE err_kinds LIKE '%attempts_exhausted%'`},
		{"history outcomes recorded twice", fmt.Sprintf(`SELECT count(*) FROM (
			SELECT 1 FROM flows.history WHERE type IN (%d, %d, %d, %d, %d, %d)
			GROUP BY execution_id, schedule_id, type HAVING count(*) > 1) d`,
			int(history.ActivityCompleted), int(history.ActivityFailed), int(history.TimerFired),
			int(history.ChildCompleted), int(history.ChildFailed), int(history.CompensationCompleted))},
	} {
		if n := count(t, cl.check, q.query); n != 0 {
			t.Errorf("%s: %d", q.what, n)
		}
	}
	cl.verifyEffects(ids)
	cl.verifyOverlaps()
}

func (cl *cluster) verifyExecutions(ids []int) {
	cl.t.Helper()
	t := cl.t
	watcher := mustEngine(t, cl.check, flows.Options{InstanceID: "watcher"})
	execs := 0
	for _, id := range ids {
		want := strings.Repeat("continued,", soakRounds-1) + soakFinalStatus(id)
		if got := cl.statuses(soakID(id)); got != want {
			t.Errorf("%s executions: %s, want %s", soakID(id), got, want)
		}
		for r := range soakRounds {
			want := "completed"
			if soakDeclines(id) && r == soakRounds-1 {
				want = "compensated"
			}
			if got := cl.statuses(soakLeafID(id, r)); got != want {
				t.Errorf("%s executions: %s, want %s", soakLeafID(id, r), got, want)
			}
		}
		execs += 2 * soakRounds
		if soakLocks(id) {
			if got := cl.statuses(soakLockedID(id)); got != "completed" {
				t.Errorf("%s executions: %s, want completed", soakLockedID(id), got)
			}
			execs++
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ended: returns at once
		out, err := soakOrder.Handle(watcher, soakID(id)).Wait(ctx)
		cancel()
		if soakDeclines(id) {
			var re *flows.RunError
			if !errors.As(err, &re) || re.Status != flows.StatusCompensated || !strings.Contains(err.Error(), "declined") {
				t.Errorf("%s: %v, want compensated after declined", soakID(id), err)
			}
		} else if err != nil || out != soakWant(id) {
			t.Errorf("%s: %d, %v; want %d", soakID(id), out, err, soakWant(id))
		}
	}
	if n := count(t, cl.check, `SELECT count(*) FROM flows.execution`); n != execs {
		t.Errorf("executions = %d, want %d", n, execs)
	}
}

func (cl *cluster) statuses(instance string) string {
	cl.t.Helper()
	rows, err := cl.check.Query(context.Background(), `SELECT status FROM flows.execution WHERE instance_id = $1 ORDER BY id`, instance)
	if err != nil {
		cl.t.Fatal(err)
	}
	defer rows.Close()
	var ss []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			cl.t.Fatal(err)
		}
		ss = append(ss, s)
	}
	if err := rows.Err(); err != nil {
		cl.t.Fatal(err)
	}
	return strings.Join(ss, ",")
}

// verifyEffects checks that every activity took effect once per key: as
// many effects as activities scheduled in the histories, every key
// attempted took effect.
func (cl *cluster) verifyEffects(ids []int) {
	cl.t.Helper()
	t := cl.t
	declined, locked := 0, 0
	for _, id := range ids {
		if soakDeclines(id) {
			declined++
		}
		if soakLocks(id) {
			locked++
		}
	}
	for _, e := range []struct {
		step string
		want int
	}{
		{"soak.step", 2 * soakRounds * len(ids)}, // the order's and its leaf's, per round
		{"soak.critical", locked},
		{"soak.decline", declined},
		{"soak.undo", 2 * declined}, // the last round's step and its leaf's
	} {
		if n := count(t, cl.check, `SELECT count(*) FROM soak_effect WHERE step = $1`, e.step); n != e.want {
			t.Errorf("%s effects = %d, want %d", e.step, n, e.want)
		}
		if n := count(t, cl.check, `SELECT count(DISTINCT key) FROM soak_attempt WHERE step = $1`, e.step); n != e.want {
			t.Errorf("%s attempted with %d keys, want %d", e.step, n, e.want)
		}
		if e.step != "soak.undo" {
			if n := count(t, cl.check, `SELECT count(*) FROM flows.history WHERE type = $1 AND name = $2`,
				int(history.ActivityScheduled), e.step); n != e.want {
				t.Errorf("%s scheduled %d times, want %d", e.step, n, e.want)
			}
		}
	}
	if n := count(t, cl.check, `SELECT count(*) FROM flows.history WHERE type = $1`, int(history.CompensationFailed)); n != 0 {
		t.Errorf("compensations failed: %d", n)
	}
}

type attemptRow struct {
	id         int64
	key, step  string
	lock       string
	pid        int
	start, end time.Time
	outcome    string
}

// verifyOverlaps checks that no two attempts of one activity (one key),
// and no two critical sections under one lock, overlapped in time. An
// attempt without a recorded end ended at the latest when its process
// exited; after waitEnded every attempt has an end.
//
// Some overlaps are expected around a SIGSTOP (pausedOverlap): a paused
// process cannot be fenced off its side effects. They are only counted.
func (cl *cluster) verifyOverlaps() {
	cl.t.Helper()
	t := cl.t
	// An attempt records its end as it returns: wait for the last ones.
	all, unknown, stats := cl.attempts()
	for deadline := time.Now().Add(30 * time.Second); unknown > 0 && time.Now().Before(deadline); {
		time.Sleep(200 * time.Millisecond)
		all, unknown, stats = cl.attempts()
	}
	cl.mu.Lock()
	stops := make(map[int][]window, len(cl.stops))
	now := time.Now()
	for pid, ws := range cl.stops {
		ws = slices.Clone(ws)
		for i := range ws {
			if ws[i].to.IsZero() { // still paused (not after waitEnded)
				ws[i].to = now
			}
		}
		stops[pid] = ws
	}
	holes := make(map[int][]window, len(cl.holes))
	for pid, ws := range cl.holes {
		holes[pid] = slices.Clone(ws)
	}
	cl.mu.Unlock()
	excused := func(a, b attemptRow) bool { return cl.pausedOverlap(stops, holes, a, b) }
	groups := map[string][]attemptRow{}
	for _, a := range all {
		groups["key "+a.key] = append(groups["key "+a.key], a)
		if a.lock != "" {
			groups["lock "+a.lock] = append(groups["lock "+a.lock], a)
		}
	}
	var bad []string
	excusedN := 0
	for g, as := range groups {
		for i, a := range as { // sorted by start
			for _, b := range as[i+1:] {
				if !b.start.Before(a.end) {
					break
				}
				if strings.HasPrefix(g, "lock ") && a.key == b.key {
					continue // one activity's attempts: checked by its key
				}
				if excused(a, b) {
					excusedN++
					continue
				}
				bad = append(bad, fmt.Sprintf("%s: attempt %d (pid %d, %s, %s..%s) and %d (pid %d, %s, from %s); faults near: %s",
					g, a.id, a.pid, a.outcome, a.start.Format("15:04:05.000"), a.end.Format("15:04:05.000"),
					b.id, b.pid, b.outcome, b.start.Format("15:04:05.000"),
					faultsNear(stops, holes, a.start.Add(-10*time.Second), b.start.Add(10*time.Second), a.pid, b.pid)))
			}
		}
	}
	dups := count(t, cl.check, `SELECT count(*) FROM soak_attempt WHERE dup`)
	t.Logf("attempts: %d by outcome %v, %d with an unknown end, %d repeated an applied effect (deduplicated), %d overlaps excused by SIGSTOP",
		len(all), stats, unknown, dups, excusedN)
	if unknown > 0 {
		t.Errorf("%d attempts of live processes without an end, after every execution ended", unknown)
	}
	if len(bad) > 0 {
		t.Errorf("%d overlapping attempts:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// faultsNear describes the SIGSTOPs and blackholes of pids between from
// and to (diagnostics).
func faultsNear(stops, holes map[int][]window, from, to time.Time, pids ...int) string {
	var b strings.Builder
	for _, pid := range pids {
		for _, f := range []struct {
			kind string
			ws   []window
		}{{"sigstop", stops[pid]}, {"blackhole", holes[pid]}} {
			for _, w := range f.ws {
				if w.from.Before(to) && w.to.After(from) {
					fmt.Fprintf(&b, "[pid %d %s %s..%s] ", pid, f.kind, w.from.Format("15:04:05.000"), w.to.Format("15:04:05.000"))
				}
			}
		}
	}
	if b.Len() == 0 {
		return "none"
	}
	return b.String()
}

// pausedOverlap reports whether attempt b, recorded as starting after a
// while a ran, overlaps a only because a process was paused (SIGSTOP)
// holding a lease that lapsed meanwhile, the paused attempt returning soon
// after the resume:
//
//   - a's process was paused before b started, while a held its lease
//     (from up to a ttl before a's recorded start, its claim, to its end):
//     the lease lapsed, b's engine took the task over, and a ran on;
//   - or b's process was paused after its claim and before it recorded its
//     start (before a started), within a ttl before that start: its lease
//     lapsed, a's engine took the task over, and b recorded its start on
//     resume, after a's.
//
// In both, the other engine's start must come at least the give-up point
// (ttl - ttl/3) after the pause began: sooner, the paused lease was still
// live. A pause of b's process while b runs, or of a's after b started,
// excuses nothing: b may start only once a's lease lapsed. Nor does a paused
// attempt running on after the resume for longer than resumeMargin: the
// resumed engine must give the lost lease up first.
func (cl *cluster) pausedOverlap(stops, holes map[int][]window, a, b attemptRow) bool {
	// On resume the overdue renewal runs at once and learns the loss, or
	// the expire timer (due within an interval) gives up the leases not
	// renewed for ttl - interval, a hanging renewal being canceled after an
	// interval: about two intervals at worst, plus 1s for -race and a
	// loaded machine.
	resumeMargin := 2*cl.ttl/3 + time.Second
	// The paused process's lease was renewed (or claimed) within an
	// interval before its renewals stopped: when the pause began, or when
	// a blackhole of that process began, if one was on within a ttl before
	// the pause (renewals stalled before the SIGSTOP). It lapses no sooner
	// than ttl - ttl/3 after that (renewed at most an interval before, for
	// ttl): another engine starting the task sooner took a live lease, and
	// the pause excuses nothing. lapseSlack covers the clocks of the
	// processes (one machine) and the recording of the pause.
	//
	// Accepted, rare false failure: b's process paused between its
	// engine's start check (the claim's age, T11.11) and the recording of
	// its start, with b's renewals stalled by something other than a
	// recorded blackhole; its lease lapsed earlier than the rule assumes.
	const lapseSlack = 100 * time.Millisecond
	lapse := cl.ttl - cl.ttl/3 - lapseSlack
	stalled := func(pid int, w window) time.Time {
		from := w.from
		for _, h := range holes[pid] {
			if h.from.Before(from) && h.to.After(w.from.Add(-cl.ttl)) {
				from = h.from
			}
		}
		return from
	}
	for _, w := range stops[a.pid] {
		if w.from.Before(b.start) && w.to.After(a.start.Add(-cl.ttl)) && !a.end.After(w.to.Add(resumeMargin)) &&
			!b.start.Before(stalled(a.pid, w).Add(lapse)) {
			return true
		}
	}
	for _, w := range stops[b.pid] {
		if w.from.Before(a.start) && w.to.After(b.start.Add(-cl.ttl)) && !b.end.After(w.to.Add(resumeMargin)) &&
			!a.start.Before(stalled(b.pid, w).Add(lapse)) {
			return true
		}
	}
	return false
}

// attempts returns the attempts recorded, by start, with their ends (an
// attempt without a recorded end ended when its process exited), the
// number of attempts whose end is unknown (left out), and the count by
// outcome.
func (cl *cluster) attempts() (all []attemptRow, unknown int, stats map[string]int) {
	cl.t.Helper()
	rows, err := cl.check.Query(context.Background(), `
		SELECT id, key, step, lock_key, pid, started, ended, outcome FROM soak_attempt ORDER BY started`)
	if err != nil {
		cl.t.Fatal(err)
	}
	defer rows.Close()
	stats = map[string]int{}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for rows.Next() {
		var a attemptRow
		var end *time.Time
		if err := rows.Scan(&a.id, &a.key, &a.step, &a.lock, &a.pid, &a.start, &end, &a.outcome); err != nil {
			cl.t.Fatal(err)
		}
		switch exit, exited := cl.exits[a.pid]; {
		case end != nil:
			a.end = *end
		case exited:
			a.end, a.outcome = exit, "exited"
		default:
			unknown++
			continue
		}
		stats[strings.SplitN(a.outcome, ":", 2)[0]]++
		all = append(all, a)
	}
	if err := rows.Err(); err != nil {
		cl.t.Fatal(err)
	}
	return all, unknown, stats
}

func (cl *cluster) dump() {
	cl.mu.Lock()
	t := cl.t
	t.Logf("faults: %v; exits %v; stops %v", cl.faults, cl.exits, cl.stops)
	cl.mu.Unlock()
	dumpQuery(t, cl.check, "execution statuses (workflow status count)",
		`SELECT workflow, status, count(*) FROM flows.execution GROUP BY 1, 2 ORDER BY 1, 2`)
	dumpQuery(t, cl.check, "unended executions (id instance status owner leased err)",
		`SELECT id, instance_id, status, owner_id, coalesce(locked_until > now(), false), err FROM flows.execution
		WHERE status IN ('running', 'compensating', 'needs_attention') ORDER BY id LIMIT 50`)
	dumpQuery(t, cl.check, "soak attempts (step outcome count)",
		`SELECT step, split_part(outcome, ':', 1), count(*) FROM soak_attempt GROUP BY 1, 2 ORDER BY 1, 2`)
	dumpQuery(t, cl.check, "locks", `SELECT key, execution_id, until > now() FROM flows.lock`)
	dumpQuery(t, cl.check, "histories of failed executions (execution instance seq type schedule attempt name err)", `
		SELECT h.execution_id, e.instance_id, h.seq, h.type, h.schedule_id, h.attempt, h.name, h.err
		FROM flows.history h JOIN flows.execution e ON e.id = h.execution_id
		WHERE e.status = 'failed' ORDER BY h.execution_id, h.seq LIMIT 200`)
}

func dumpQuery(t *testing.T, pool *pgxpool.Pool, title, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Logf("%s: %v", title, err)
		return
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Logf("%s: %v", title, err)
			return
		}
		fmt.Fprintln(&b, vals...)
	}
	t.Logf("%s:\n%s", title, b.String())
}
