// Package modulith_test enforces the example's dependency rule: the domain
// modules do not know the workflow engine exists, nor each other.
package modulith_test

import (
	"errors"
	"os/exec"
	"strings"
	"testing"
)

const (
	repo    = "github.com/imdat99/go-flows"
	example = repo + "/examples/modulith"
)

// pkg is what go list reports of a package.
type pkg struct {
	path        string
	deps        []string // transitive, production code
	testImports []string // direct imports of its tests (both packages)
}

// list runs go list on pattern (relative to this directory).
func list(t *testing.T, pattern string) []pkg {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list",
		"-f", `{{.ImportPath}}|{{join .Deps " "}}|{{join .TestImports " "}} {{join .XTestImports " "}}`,
		pattern).Output()
	if err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			t.Fatalf("go list %s: %v\n%s", pattern, err, ee.Stderr)
		}
		t.Fatalf("go list %s: %v", pattern, err)
	}
	var pkgs []pkg
	for line := range strings.Lines(string(out)) {
		parts := strings.Split(strings.TrimSpace(line), "|")
		if len(parts) != 3 {
			t.Fatalf("go list: unexpected line %q", line)
		}
		pkgs = append(pkgs, pkg{path: parts[0], deps: strings.Fields(parts[1]), testImports: strings.Fields(parts[2])})
	}
	if len(pkgs) == 0 {
		t.Fatalf("go list %s: no packages", pattern)
	}
	return pkgs
}

// moduleOf returns the module ("orders") of a package under modules/, or "".
func moduleOf(path string) string {
	rest, ok := strings.CutPrefix(path, example+"/modules/")
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(rest, "/")
	return name
}

// allowedForModule reports whether a package of module m may depend on dep:
// anything outside this repository (the standard library, pgx, goose), its
// own packages, and the example's shared platform (schema, outbox), which
// knows nothing of the engine either. Not go-flows (flows, idem, flowtest,
// otelflows, internal/...), not another module, not the process layer, not
// the composition root.
func allowedForModule(m, dep string) bool {
	switch {
	case dep != repo && !strings.HasPrefix(dep, repo+"/"):
		return true
	case moduleOf(dep) == m:
		return true
	case strings.HasPrefix(dep, example+"/internal/platform/"):
		return true
	}
	return false
}

// TestModulesDoNotKnowTheEngine: no package under modules/ depends, even
// transitively, on a go-flows package or on another module. Cross-module
// work goes through the process layer (processes/), the only code that
// imports the engine.
func TestModulesDoNotKnowTheEngine(t *testing.T) {
	for _, p := range list(t, "./modules/...") {
		m := moduleOf(p.path)
		for _, dep := range p.deps {
			if !allowedForModule(m, dep) {
				t.Errorf("%s depends on %s", strings.TrimPrefix(p.path, example+"/"), dep)
			}
		}
		// Tests may use the repository's test database helper too (plain
		// pgx, no engine), nothing else of go-flows.
		for _, imp := range p.testImports {
			if imp != repo+"/internal/testdb" && !allowedForModule(m, imp) {
				t.Errorf("tests of %s import %s", strings.TrimPrefix(p.path, example+"/"), imp)
			}
		}
	}
}

// TestPlatformDoesNotKnowTheEngine: the shared platform packages the modules
// may use stay free of the engine too.
func TestPlatformDoesNotKnowTheEngine(t *testing.T) {
	for _, p := range list(t, "./internal/platform/...") {
		for _, dep := range p.deps {
			if strings.HasPrefix(dep, repo+"/") && !strings.HasPrefix(dep, example+"/internal/platform/") {
				t.Errorf("%s depends on %s", strings.TrimPrefix(p.path, example+"/"), dep)
			}
		}
	}
}
