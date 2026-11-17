//go:build upgrade

// Package upgrade_test upgrades a database in use from release FromTag to
// HEAD: a driver built from FromTag (olddriver, against the release's own
// tree) takes executions to interesting points (an activity retrying, a
// timer pending, signals awaited, a lock held, compensation pending or under
// way, a continue-as-new chain, child workflows) and stops its engine; then
// HEAD's Migrate runs and a HEAD engine finishes them. Both run the same
// definitions (package defs). Run with `make upgrade` (needs git and the
// Go toolchain, and Postgres as for `make test`).
package upgrade_test

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	flows "github.com/imdat99/go-flows/flows"
	"github.com/imdat99/go-flows/internal/testdb"
	"github.com/imdat99/go-flows/test/upgrade/defs"
)

// Out of scope: an old engine that crashed with live leases, or work in
// flight while Migrate runs. CHANGELOG says to stop old engines first.

// FromTag is the release upgraded from: the last one before migration
// 00016_key_epoch, whose executions keep the legacy activity key.
const FromTag = "v0.1.0"

// waitFor bounds each wait for an instance; with every wait timing out
// they stay within the go test timeout of `make upgrade`, so the dump of
// the database (a Cleanup) still runs.
const waitFor = time.Minute

// buildOld builds olddriver against the tree of FromTag: the tag's files,
// plus package defs and olddriver copied in at their paths (same module
// path, so the imports resolve to the release's packages).
func buildOld(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(runCmd(t, "", "git", "rev-parse", "--show-toplevel"))
	if err := exec.CommandContext(t.Context(), "git", "-C", root, "rev-parse", "--verify", "--quiet", FromTag+"^{commit}").Run(); err != nil {
		t.Fatalf("tag %s not found: fetch tags (git fetch --tags; in CI, actions/checkout with fetch-depth: 0)", FromTag)
	}
	tmp := t.TempDir()
	src := filepath.Join(tmp, FromTag)
	if err := os.MkdirAll(src, 0o750); err != nil {
		t.Fatal(err)
	}
	tarball := filepath.Join(tmp, "src.tar")
	runCmd(t, root, "git", "archive", "--format=tar", "-o", tarball, FromTag)
	runCmd(t, src, "tar", "-xf", tarball)
	for _, dir := range []string{"test/upgrade/defs", "test/upgrade/olddriver"} {
		copyGoFiles(t, filepath.Join(root, dir), filepath.Join(src, dir))
	}
	bin := filepath.Join(tmp, "olddriver")
	runCmd(t, src, "go", "build", "-o", bin, "./test/upgrade/olddriver")
	return bin
}

func copyGoFiles(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(to, 0o750); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(from, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func runCmd(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=") // the release's go.mod as is
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return string(out)
}

// expect describes the activity of one tag: the instance it belongs to,
// whether its execution predates the key epoch (legacy key), the phase
// whose attempt succeeded, and the phase of its compensation ("" for
// none).
type expect struct {
	instance  string
	legacy    bool
	okPhase   string
	compPhase string
}

const (
	oldP = defs.PhaseOld
	newP = defs.PhaseNew
)

var expects = map[string]expect{
	"retry":          {"up-retry", true, newP, ""},
	"timer-a":        {"up-timer", true, oldP, ""},
	"timer-b":        {"up-timer", true, newP, ""},
	"signal-a":       {"up-signal", true, oldP, ""},
	"signal-b":       {"up-signal", true, newP, ""},
	"lock-a":         {"up-lock", true, oldP, ""},
	"lock-b":         {"up-lock", true, newP, ""},
	"saga-debit":     {"up-saga", true, oldP, newP},
	"saga-credit":    {"up-saga", true, oldP, newP},
	"sagamid-debit":  {"up-sagamid", true, oldP, newP},
	"can-0":          {"up-can", true, oldP, ""},
	"can-1":          {"up-can", true, newP, ""},
	"can-2":          {"up-can", false, newP, ""},
	"parent-c-a":     {"up-parent-c", true, oldP, ""},
	"parent-c-b":     {"up-parent-c", true, newP, ""},
	"parent":         {"up-parent", true, newP, ""},
	"pf-p":           {"up-pf", true, oldP, newP},
	"pf-c-a":         {"up-pf-c", true, oldP, newP},
	"new-retry":      {"up-new-retry", false, newP, ""},
	"new-saga-debit": {"up-new-saga", false, newP, newP},
	"new-saga-credit": {
		"up-new-saga", false, newP, newP,
	},
	"probe-2": {"up-probe-2", false, newP, ""},
}

func TestUpgrade(t *testing.T) {
	bin := buildOld(t)
	pool := testdb.New(t)
	ctx := t.Context()
	if _, err := pool.Exec(ctx, defs.Schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			dump(t, pool)
		}
	})

	// Phase 1: the old release.
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = append(os.Environ(), "FLOWS_UPGRADE_DSN="+testdb.DSN(pool))
	out, err := cmd.CombinedOutput()
	t.Logf("old driver (%s):\n%s", FromTag, out)
	if err != nil {
		t.Fatalf("old driver: %v", err)
	}
	unready, err := defs.Unready(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(unready) > 0 {
		t.Fatalf("after the old engine stopped, not at the upgrade point: %s", strings.Join(unready, "; "))
	}
	var leased int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM flows.execution WHERE locked_until > now())
		+ (SELECT count(*) FROM flows.activity_task WHERE owner_id <> '' AND locked_until > now())`).
		Scan(&leased); err != nil {
		t.Fatal(err)
	}
	if leased > 0 {
		t.Errorf("the old engine stopped with %d leases live", leased)
	}
	var oldMax int64
	if err := pool.QueryRow(ctx, `SELECT max(id) FROM flows.execution`).Scan(&oldMax); err != nil {
		t.Fatal(err)
	}

	// Phase 2: HEAD.
	if err := flows.Migrate(ctx, pool); err != nil {
		t.Fatalf("HEAD migrate: %v", err)
	}
	var epoch int64
	if err := pool.QueryRow(ctx, `SELECT first_execution_id FROM flows.key_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if epoch <= oldMax {
		t.Errorf("key epoch %d, want past the old executions (up to %d)", epoch, oldMax)
	}
	defs.Setup(pool, defs.PhaseNew)
	opts := defs.Options("new", slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn})))
	opts.CacheSize = -1 // every task replays the whole history, the old part included
	eng, err := flows.New(pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := eng.Stop(sctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	// The lock taken by the old engine is still held.
	if got := waitOut(t, defs.LockProbe, eng, "up-probe-1", "probe-1"); got != "held" {
		t.Errorf("lock probe before release: %q, want held", got)
	}
	start(t, defs.Retry, eng, "up-new-retry", defs.Step{Tag: "new-retry", FailFirst: 2})
	start(t, defs.Saga, eng, "up-new-saga", "new-saga")
	for _, s := range []struct{ instance, name, payload string }{
		{"up-signal", "go", "signal-b"},
		{"up-lock", "release", ""},
		{"up-saga", "proceed", ""},
		{"up-new-saga", "proceed", ""},
		{"up-can", "go", ""},
		{"up-parent-c", "child-go", ""},
		{"up-pf", "fail", ""},
	} {
		if err := eng.Signal(ctx, s.instance, s.name, s.payload); err != nil {
			t.Fatalf("signal %s to %s: %v", s.name, s.instance, err)
		}
	}

	wantOut(t, defs.Retry, eng, "up-retry", "done:retry")
	wantOut(t, defs.Timer, eng, "up-timer", "done:timer-b")
	wantOut(t, defs.Signal, eng, "up-signal", "done:signal-b")
	wantOut(t, defs.Locked, eng, "up-lock", "done:lock-b")
	wantOut(t, defs.Can, eng, "up-can", "done:can-2")
	wantOut(t, defs.Child, eng, "up-parent-c", "child:done:parent-c-b")
	wantOut(t, defs.Parent, eng, "up-parent", "child:done:parent-c-b|done:parent")
	wantOut(t, defs.Retry, eng, "up-new-retry", "done:new-retry")
	wantCompensated(t, defs.Saga, eng, "up-saga")
	wantCompensated(t, defs.SagaMid, eng, "up-sagamid")
	wantCompensated(t, defs.ParentFail, eng, "up-pf")
	wantCompensated(t, defs.Child2, eng, "up-pf-c")
	wantCompensated(t, defs.Saga, eng, "up-new-saga")
	// Released at the end of up-lock.
	if got := waitOut(t, defs.LockProbe, eng, "up-probe-2", "probe-2"); got != "free" {
		t.Errorf("lock probe after release: %q, want free", got)
	}

	checkEnded(t, pool)
	checkActivities(t, pool, epoch)
	checkCompensationOrder(t, pool)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

func start[I, O any](t *testing.T, wf *flows.Workflow[I, O], e *flows.Engine, id string, in I) {
	t.Helper()
	if _, err := wf.Start(t.Context(), e, id, in); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
}

func wait[O any](t *testing.T, wf *flows.Workflow[string, O], e *flows.Engine, id string) (O, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	return wf.Handle(e, id).Wait(ctx)
}

func waitOut(t *testing.T, wf *flows.Workflow[string, string], e *flows.Engine, id, in string) string {
	t.Helper()
	start(t, wf, e, id, in)
	out, err := wait(t, wf, e, id)
	if err != nil {
		t.Fatalf("%s: %v", id, err)
	}
	return out
}

func wantOut[I any](t *testing.T, wf *flows.Workflow[I, string], e *flows.Engine, id, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	got, err := wf.Handle(e, id).Wait(ctx)
	if err != nil || got != want {
		t.Errorf("%s: (%q, %v), want %q", id, got, err, want)
	}
}

func wantCompensated[I any](t *testing.T, wf *flows.Workflow[I, string], e *flows.Engine, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	_, err := wf.Handle(e, id).Wait(ctx)
	var re *flows.RunError
	if !errors.As(err, &re) || re.Status != flows.StatusCompensated {
		t.Errorf("%s: %v, want compensated", id, err)
	}
}

// checkEnded: every execution ended as expected (none parked or failed),
// and nothing of them is left behind.
func checkEnded(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := t.Context()
	rows, err := pool.Query(ctx, `SELECT instance_id, id, status, err FROM flows.execution
		WHERE status NOT IN ('completed', 'compensated', 'continued') ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var inst, status, msg string
		var id int64
		if err := rows.Scan(&inst, &id, &status, &msg); err != nil {
			t.Fatal(err)
		}
		t.Errorf("execution %d of %s: %s (%s)", id, inst, status, msg)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM flows.lock) + (SELECT count(*) FROM flows.pending_event)
		+ (SELECT count(*) FROM flows.activity_task)`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left > 0 {
		t.Errorf("%d locks, pending events or activity tasks left", left)
	}
}

type attempt struct {
	key, phase    string
	comp, ok, app bool
}

// checkActivities: each activity (tag) saw one key across its attempts,
// both engines and its compensation, in the format of its execution
// (legacy before the key epoch); it succeeded once, and was compensated
// once when expected; the effect of each key and kind applied once.
func checkActivities(t *testing.T, pool *pgxpool.Pool, epoch int64) {
	t.Helper()
	byTag := loadAttempts(t, pool)
	for tag := range byTag {
		if _, ok := expects[tag]; !ok {
			t.Errorf("unexpected activity %q", tag)
		}
	}
	keyOf := map[string]string{}
	for _, tag := range slices.Sorted(maps.Keys(expects)) {
		if as := byTag[tag]; len(as) == 0 {
			t.Errorf("%s: never ran", tag)
		} else {
			keyOf[tag] = checkTag(t, pool, tag, as, epoch)
		}
	}
	// Attempts that failed on purpose ran on the old engine only (retry,
	// sagamid's compensator) or before the success (new-retry).
	for tag, wantFailed := range map[string]string{"retry": oldP, "sagamid-debit": oldP, "new-retry": newP} {
		n := 0
		for _, a := range byTag[tag] {
			if a.ok {
				continue
			}
			n++
			if a.phase != wantFailed {
				t.Errorf("%s: failed attempt on the %s engine", tag, a.phase)
			}
		}
		if n < 2 {
			t.Errorf("%s: %d failed attempts, want at least 2", tag, n)
		}
	}
	checkEffects(t, pool, byTag, keyOf)
}

func loadAttempts(t *testing.T, pool *pgxpool.Pool) map[string][]attempt {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT tag, key, phase, compensate, ok, applied FROM upgrade_attempt ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	byTag := map[string][]attempt{}
	for rows.Next() {
		var tag string
		var a attempt
		if err := rows.Scan(&tag, &a.key, &a.phase, &a.comp, &a.ok, &a.app); err != nil {
			t.Fatal(err)
		}
		byTag[tag] = append(byTag[tag], a)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return byTag
}

// checkTag checks the attempts of one activity and returns its key.
func checkTag(t *testing.T, pool *pgxpool.Pool, tag string, as []attempt, epoch int64) string {
	t.Helper()
	want := expects[tag]
	key := as[0].key
	t.Logf("%-16s key %-32s attempts %d", tag, key, len(as))
	for _, a := range as {
		if a.key != key {
			t.Errorf("%s: keys %q and %q", tag, key, a.key)
		}
	}
	checkKey(t, pool, tag, key, want, epoch)
	for _, comp := range []bool{false, true} {
		wantPhase := want.okPhase
		if comp {
			wantPhase = want.compPhase
		}
		var oks []attempt
		for _, a := range as {
			if a.comp == comp && a.ok {
				oks = append(oks, a)
			}
		}
		switch {
		case wantPhase == "" && len(oks) > 0:
			t.Errorf("%s (compensation %v): ran %d times, want never", tag, comp, len(oks))
		case wantPhase != "" && len(oks) != 1:
			t.Errorf("%s (compensation %v): succeeded %d times, want once", tag, comp, len(oks))
		case wantPhase != "" && oks[0].phase != wantPhase:
			t.Errorf("%s (compensation %v): succeeded on the %s engine, want %s", tag, comp, oks[0].phase, wantPhase)
		}
	}
	return key
}

// checkEffects: each key and kind applied its effect once. The legacy
// keys of the executions of a continue-as-new chain repeat (the bug T10.19
// fixed, for executions after the key epoch only): can-1 has can-0's key,
// so its effect is deduplicated against can-0's, as before the upgrade.
func checkEffects(t *testing.T, pool *pgxpool.Pool, byTag map[string][]attempt, keyOf map[string]string) {
	t.Helper()
	if keyOf["can-0"] != keyOf["can-1"] {
		t.Errorf("can-0 and can-1: legacy keys %q and %q, want the same", keyOf["can-0"], keyOf["can-1"])
	}
	for tag := range expects {
		for _, a := range byTag[tag] {
			if a.ok && a.app != (tag != "can-1") {
				t.Errorf("%s (compensation %v): effect applied %v", tag, a.comp, a.app)
			}
		}
	}
	var effects int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM upgrade_effect`).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for tag, want := range expects {
		seen[keyOf[tag]+"/0"] = true
		if want.compPhase != "" {
			seen[keyOf[tag]+"/1"] = true
		}
	}
	if effects != len(seen) {
		t.Errorf("%d effects, want %d", effects, len(seen))
	}
}

// checkKey checks the format of key: "instance:schedule:name" for an
// execution before the epoch, "instance:schedule:name:executionID" after,
// the execution being one of the instance's, at or after the epoch.
func checkKey(t *testing.T, pool *pgxpool.Pool, tag, key string, want expect, epoch int64) {
	t.Helper()
	parts := strings.Split(key, ":")
	if parts[0] != want.instance || len(parts) < 3 || parts[2] != defs.StepAct.Name() {
		t.Errorf("%s: key %q, want %s:<schedule>:%s...", tag, key, want.instance, defs.StepAct.Name())
		return
	}
	if want.legacy {
		if len(parts) != 3 {
			t.Errorf("%s: key %q, want the legacy format (execution before the epoch)", tag, key)
		}
		return
	}
	if len(parts) != 4 {
		t.Errorf("%s: key %q, want the execution id at the end", tag, key)
		return
	}
	id, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || id < epoch {
		t.Errorf("%s: key %q: execution id before the epoch %d", tag, key, epoch)
		return
	}
	var inst string
	if err := pool.QueryRow(t.Context(), `SELECT instance_id FROM flows.execution WHERE id = $1`, id).Scan(&inst); err != nil ||
		inst != want.instance {
		t.Errorf("%s: key %q: execution %d is %q (%v)", tag, key, id, inst, err)
	}
}

// checkCompensationOrder: compensations run in reverse order.
func checkCompensationOrder(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, order := range [][]string{{"saga-credit", "saga-debit"}, {"new-saga-credit", "new-saga-debit"}, {"pf-c-a", "pf-p"}} {
		var got []string
		rows, err := pool.Query(t.Context(), `SELECT tag FROM upgrade_attempt WHERE compensate AND ok AND tag = ANY($1)
			ORDER BY id`, order)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var tag string
			if err := rows.Scan(&tag); err != nil {
				t.Fatal(err)
			}
			got = append(got, tag)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, order) {
			t.Errorf("compensation order %v, want %v", got, order)
		}
	}
}

// dump logs the executions and attempts of a failed run.
func dump(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{
		`SELECT id, instance_id, workflow, status, err, owner_id, locked_until, not_before FROM flows.execution ORDER BY id`,
		`SELECT id, instance_id, schedule_id, compensate, name, attempt, owner_id, visible_at FROM flows.activity_task ORDER BY id`,
		`SELECT p.id, p.execution_id, p.type, p.schedule_id, p.name, p.visible_at, now() FROM flows.pending_event p ORDER BY id`,
		`SELECT id, tag, key, compensate, phase, ok, applied FROM upgrade_attempt ORDER BY id`,
		`SELECT h.execution_id, h.seq, h.type, h.schedule_id, h.name, h.err FROM flows.history h ORDER BY 1, 2`,
	} {
		rows, err := pool.Query(ctx, q)
		if err != nil {
			t.Logf("dump: %v", err)
			continue
		}
		var b strings.Builder
		b.WriteString(q + "\n")
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				break
			}
			for i, v := range vals {
				if i > 0 {
					b.WriteString(" | ")
				}
				b.WriteString(toString(v))
			}
			b.WriteString("\n")
		}
		rows.Close()
		t.Log(b.String())
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case bool:
		return strconv.FormatBool(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	default:
		return "?"
	}
}
