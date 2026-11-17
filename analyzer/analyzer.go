// Package analyzer checks workflow code for what breaks deterministic
// replay: goroutines, native channels and select, map iteration, the wall
// clock, randomness and sync primitives.
//
// Ported from go-workflows analyzer (MIT, see NOTICE), for the go-flows
// API. Workflow code is any function or function literal whose first
// parameter is flows.Ctx (workflow functions, coroutines started with
// flows.Go, helpers), recognized by type, not by import name. Function
// literals passed to flows.SideEffect (inline, or through a local variable
// used for nothing else) are not workflow code (their value is recorded),
// nor are activities (their first parameter is a
// context.Context).
//
// Use it as a golangci-lint module plugin ("flowsvet") or with go vet:
//
//	go install github.com/imdat99/go-flows/analyzer/cmd/flowsvet@latest
//	go vet -vettool=$(which flowsvet) ./...
package analyzer

import (
	"go/ast"
	"go/token"
	"go/types"

	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"
)

// FlowsPath is the import path of the flows package.
const FlowsPath = "github.com/imdat99/go-flows/flows"

// Analyzer reports non-deterministic code in workflows.
var Analyzer = &analysis.Analyzer{
	Name: "flowsvet",
	Doc:  "reports code that breaks the deterministic replay of go-flows workflows",
	Run:  run,
}

func init() { register.Plugin("flowsvet", newPlugin) }

func newPlugin(any) (register.LinterPlugin, error) { return plugin{}, nil }

type plugin struct{}

func (plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return []*analysis.Analyzer{Analyzer}, nil
}

func (plugin) GetLoadMode() string { return register.LoadModeTypesInfo }

func run(pass *analysis.Pass) (any, error) {
	if pass.Pkg.Path() == FlowsPath {
		return nil, nil //nolint:nilnil // the engine implements the deterministic APIs itself
	}
	for _, f := range pass.Files {
		ast.Inspect(f, func(n ast.Node) bool {
			var ft *ast.FuncType
			var body *ast.BlockStmt
			switch fn := n.(type) {
			case *ast.FuncDecl:
				ft, body = fn.Type, fn.Body
			case *ast.FuncLit:
				ft, body = fn.Type, fn.Body
			default:
				return true
			}
			if body == nil || !isWorkflowFunc(pass, ft) {
				return true
			}
			c := checker{pass: pass, sideEffectFns: sideEffectFuncs(pass, body)}
			c.checkBody(body)
			return false // nested workflow code was checked with it
		})
	}
	return nil, nil //nolint:nilnil // an analyzer without a result
}

// isWorkflowFunc reports whether the first parameter is flows.Ctx.
func isWorkflowFunc(pass *analysis.Pass, ft *ast.FuncType) bool {
	if ft.Params == nil || len(ft.Params.List) == 0 {
		return false
	}
	return isFlows(pass.TypesInfo.TypeOf(ft.Params.List[0].Type), "Ctx")
}

// isFlows reports whether t is the named type flows.<name>.
func isFlows(t types.Type, name string) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == FlowsPath && obj.Name() == name
}

// checker checks the body of one workflow function.
type checker struct {
	pass *analysis.Pass
	// sideEffectFns are the function literals held in variables that are
	// only passed to flows.SideEffect: not workflow code.
	sideEffectFns map[*ast.FuncLit]bool
}

// checkExpr checks an expression as workflow code.
func (c checker) checkExpr(e ast.Expr) {
	c.checkBody(&ast.BlockStmt{List: []ast.Stmt{&ast.ExprStmt{X: e}}})
}

func (c checker) checkBody(body *ast.BlockStmt) {
	pass := c.pass
	ast.Inspect(body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GoStmt:
			pass.Reportf(n.Pos(), "use flows.Go instead of a go statement in workflow code")
		case *ast.SelectStmt:
			pass.Reportf(n.Pos(), "select is not deterministic in workflow code: use flows.Select")
		case *ast.SendStmt:
			pass.Report(analysis.Diagnostic{Pos: n.Pos(), Message: chanMsg + ": " + chanAdvice})
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				pass.Report(analysis.Diagnostic{Pos: n.Pos(), Message: chanMsg + ": " + chanAdvice})
			}
		case *ast.RangeStmt:
			checkRange(pass, n)
		case *ast.ValueSpec, *ast.AssignStmt:
			checkDefs(pass, n)
		case *ast.FuncLit:
			return !c.sideEffectFns[n]
		case *ast.CallExpr:
			if isMakeChan(pass, n) {
				pass.Reportf(n.Pos(), "%s", typeReport(pass.TypesInfo.TypeOf(n), ""))
			}
			return c.checkCall(n)
		case *ast.SelectorExpr:
			checkFuncValue(pass, n)
			c.checkExpr(n.X)
			return false // not n.Sel again
		case *ast.Ident:
			checkFuncValue(pass, n)
		}
		return true
	})
}

// checkRange reports ranging over a map (directly, or through maps.All,
// Keys or Values) or a channel.
func checkRange(pass *analysis.Pass, n *ast.RangeStmt) {
	if call, ok := n.X.(*ast.CallExpr); ok {
		if fn := calledFunc(pass, call.Fun); fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == "maps" {
			switch fn.Name() {
			case "All", "Keys", "Values":
				pass.Reportf(n.Pos(), "iterating over a map is not deterministic in workflow code: iterate over sorted keys (slices.Sorted(maps.Keys(m)))")
			}
			return
		}
	}
	t := pass.TypesInfo.TypeOf(n.X)
	if t == nil {
		return
	}
	switch coreType(t).(type) {
	case *types.Map:
		pass.Reportf(n.Pos(), "iterating over a map is not deterministic in workflow code: iterate over sorted keys")
	case *types.Chan:
		pass.Report(analysis.Diagnostic{Pos: n.Pos(), Message: chanMsg + ": " + chanAdvice})
	}
}

// coreType is t's underlying type, or for a type parameter the common
// underlying type of its constraint's terms.
func coreType(t types.Type) types.Type {
	t = types.Unalias(t)
	tp, ok := t.(*types.TypeParam)
	if !ok {
		return t.Underlying()
	}
	iface, ok := tp.Constraint().Underlying().(*types.Interface)
	if !ok {
		return nil
	}
	for e := range iface.EmbeddedTypes() {
		u, ok := e.(*types.Union)
		if !ok {
			return e.Underlying()
		}
		for term := range u.Terms() {
			return term.Type().Underlying()
		}
	}
	return nil
}

// checkFuncValue reports a forbidden function used as a value (now :=
// time.Now; a callback): calls are reported by checkCall, which does not
// descend into the callee.
func checkFuncValue(pass *analysis.Pass, e ast.Expr) {
	var id *ast.Ident
	switch x := e.(type) {
	case *ast.SelectorExpr:
		id = x.Sel
	case *ast.Ident:
		id = x
	}
	fn, ok := pass.TypesInfo.Uses[id].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return
	}
	if msg := forbiddenFunc(fn); msg != "" {
		pass.Reportf(e.Pos(), "%s", msg)
	}
}

// checkDefs reports variables of types that do not belong in workflow code:
// channels, sync primitives, and structs holding them in a field.
func checkDefs(pass *analysis.Pass, n ast.Node) {
	var idents []*ast.Ident
	var values []ast.Expr
	switch d := n.(type) {
	case *ast.ValueSpec:
		idents, values = d.Names, d.Values
	case *ast.AssignStmt:
		if d.Tok != token.DEFINE {
			return
		}
		for _, l := range d.Lhs {
			id, _ := l.(*ast.Ident)
			idents = append(idents, id) // nil keeps idents aligned with values
		}
		values = d.Rhs
	}
	for i, id := range idents {
		if id == nil {
			continue
		}
		obj := pass.TypesInfo.Defs[id]
		if obj == nil {
			continue
		}
		if len(values) == len(idents) && isMakeChan(pass, values[i]) {
			continue // reported at the make call
		}
		t := types.Unalias(obj.Type())
		if p, ok := t.(*types.Pointer); ok {
			t = types.Unalias(p.Elem())
		}
		t = arrayElem(t)
		if msg := typeReport(t, ""); msg != "" {
			pass.Reportf(id.Pos(), "%s", msg)
			continue
		}
		checkFields(pass, id, t, "")
	}
}

// checkFields reports, at id, the fields of t that hold a channel or a sync
// primitive, through nested structs and arrays. Only structs declared in
// the package being checked and anonymous ones are looked into (not
// another package's internals), and not through pointers (the methods
// called through them are reported).
func checkFields(pass *analysis.Pass, id *ast.Ident, t types.Type, prefix string) {
	if named, ok := t.(*types.Named); ok && named.Obj().Pkg() != pass.Pkg {
		return
	}
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return
	}
	for f := range st.Fields() {
		ft := arrayElem(types.Unalias(f.Type()))
		name := prefix + f.Name()
		if msg := typeReport(ft, name); msg != "" {
			pass.Reportf(id.Pos(), "%s", msg)
			continue
		}
		checkFields(pass, id, ft, name+".")
	}
}

// arrayElem is the element type of t if t is an array (of arrays), or t.
func arrayElem(t types.Type) types.Type {
	for {
		a, ok := t.Underlying().(*types.Array)
		if !ok {
			return t
		}
		t = types.Unalias(a.Elem())
	}
}

// typeReport returns the report for a value of type t in workflow code (a
// channel or a sync primitive), or "". field names the struct field
// holding the value, if any.
func typeReport(t types.Type, field string) string {
	if field != "" {
		field = " (field " + field + ")"
	}
	if _, ok := coreType(t).(*types.Chan); ok {
		return chanMsg + field + ": " + chanAdvice
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "sync" {
		return ""
	}
	switch name := named.Obj().Name(); name {
	case "WaitGroup":
		return "sync.WaitGroup" + field + " blocks workflow code: use flows.WaitGroup"
	case "Mutex", "RWMutex", "Cond", "Once":
		return "sync." + name + field + " is not needed in workflow code (coroutines never run in parallel) and can block it"
	}
	return ""
}

// isMakeChan reports whether e calls the builtin make to create a channel.
func isMakeChan(pass *analysis.Pass, e ast.Expr) bool {
	call, ok := ast.Unparen(e).(*ast.CallExpr)
	if !ok {
		return false
	}
	id, ok := ast.Unparen(call.Fun).(*ast.Ident)
	if !ok {
		return false
	}
	if b, ok := pass.TypesInfo.Uses[id].(*types.Builtin); !ok || b.Name() != "make" {
		return false
	}
	t := pass.TypesInfo.TypeOf(call)
	if t == nil {
		return false
	}
	_, ok = coreType(t).(*types.Chan)
	return ok
}

// sideEffectFuncs returns the function literals assigned to local variables
// whose only uses are as the function argument of flows.SideEffect: their
// value is recorded, so they are not workflow code. The literals of a
// variable also called, or passed anywhere else, stay checked.
func sideEffectFuncs(pass *analysis.Pass, body *ast.BlockStmt) map[*ast.FuncLit]bool {
	lits := funcLitVars(pass, body)
	if len(lits) == 0 {
		return nil
	}
	var skip map[*ast.FuncLit]bool
	for v, only := range sideEffectOnly(pass, body, lits) {
		if !only {
			continue
		}
		if skip == nil {
			skip = map[*ast.FuncLit]bool{}
		}
		for _, l := range lits[v] {
			skip[l] = true
		}
	}
	return skip
}

// funcLitVars returns the function literals assigned (or given as initial
// value) to each variable declared in body.
func funcLitVars(pass *analysis.Pass, body *ast.BlockStmt) map[*types.Var][]*ast.FuncLit {
	var lits map[*types.Var][]*ast.FuncLit
	bind := func(l, r ast.Expr) {
		id, ok := l.(*ast.Ident)
		lit, isLit := ast.Unparen(r).(*ast.FuncLit)
		if !ok || !isLit {
			return
		}
		obj := pass.TypesInfo.Defs[id]
		if obj == nil {
			obj = pass.TypesInfo.Uses[id] // assigned, or redeclared by :=
		}
		// Only variables declared in the body: the uses of others are not
		// all seen.
		v, ok := obj.(*types.Var)
		if !ok || v.Pos() < body.Pos() || v.Pos() >= body.End() {
			return
		}
		if lits == nil {
			lits = map[*types.Var][]*ast.FuncLit{}
		}
		lits[v] = append(lits[v], lit)
	}
	ast.Inspect(body, func(n ast.Node) bool {
		switch d := n.(type) {
		case *ast.AssignStmt:
			if len(d.Lhs) == len(d.Rhs) && (d.Tok == token.DEFINE || d.Tok == token.ASSIGN) {
				for i := range d.Lhs {
					bind(d.Lhs[i], d.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(d.Names) == len(d.Values) {
				for i := range d.Names {
					bind(d.Names[i], d.Values[i])
				}
			}
		}
		return true
	})
	return lits
}

// sideEffectOnly maps each variable of lits used in body to whether all its
// uses (assignments aside) are as the function argument of
// flows.SideEffect.
func sideEffectOnly(pass *analysis.Pass, body *ast.BlockStmt, lits map[*types.Var][]*ast.FuncLit) map[*types.Var]bool {
	// Parents are visited before their children: an identifier is known to
	// be assigned to, or recorded, when it is reached.
	assigned := map[*ast.Ident]bool{}
	recorded := map[*ast.Ident]bool{}
	only := map[*types.Var]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			if n.Tok == token.DEFINE || n.Tok == token.ASSIGN {
				for _, l := range n.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						assigned[id] = true
					}
				}
			}
		case *ast.CallExpr:
			if isSideEffect(pass, n) && len(n.Args) > 1 {
				if id, ok := ast.Unparen(n.Args[1]).(*ast.Ident); ok {
					recorded[id] = true
				}
			}
		case *ast.Ident:
			v, ok := pass.TypesInfo.Uses[n].(*types.Var)
			if !ok || lits[v] == nil || assigned[n] {
				return true
			}
			prev, seen := only[v]
			only[v] = recorded[n] && (prev || !seen)
		}
		return true
	})
	return only
}

// isSideEffect reports whether call calls flows.SideEffect.
func isSideEffect(pass *analysis.Pass, call *ast.CallExpr) bool {
	fn := calledFunc(pass, call.Fun)
	return fn != nil && fn.Pkg() != nil && fn.Pkg().Path() == FlowsPath && fn.Name() == "SideEffect"
}

// forbidden maps package path and function to the advice given.
var forbidden = map[string]map[string]string{
	"time": {
		"Now": "use flows.Now", "Since": "use flows.Now", "Until": "use flows.Now",
		"Sleep": "use flows.Sleep", "After": "use flows.NewTimer", "AfterFunc": "use flows.NewTimer",
		"NewTimer": "use flows.NewTimer", "NewTicker": "use flows.NewTimer", "Tick": "use flows.NewTimer",
	},
	"math/rand":    nil, // every function but the constructors (randomConstructors): use flows.Rand
	"math/rand/v2": nil,
	"crypto/rand":  nil,
	"os": {
		"Getenv": "read configuration in an activity or pass it as input", "LookupEnv": "read configuration in an activity or pass it as input",
		"Environ":  "read configuration in an activity or pass it as input",
		"Hostname": "read it in an activity or pass it as input", "Getpid": "read it in an activity or pass it as input",
		"Getwd": "read it in an activity or pass it as input",
	},
	"context": {
		"WithTimeout":  "deadlines use the wall clock: use flows.NewTimer with flows.Select",
		"WithDeadline": "deadlines use the wall clock: use flows.NewTimer with flows.Select",
	},
	// Random ids (the name-based NewMD5/NewSHA1 derive from their input).
	"github.com/google/uuid": {
		"New": idAdvice, "NewString": idAdvice, "NewRandom": idAdvice, "NewRandomFromReader": idAdvice,
		"NewUUID": idAdvice, "NewV6": idAdvice, "NewV7": idAdvice, "NewV7FromReader": idAdvice,
		"NewDCEGroup": idAdvice, "NewDCEPerson": idAdvice, "NewDCESecurity": idAdvice,
	},
}

// chanMsg and chanAdvice make the report of a native channel.
const (
	chanMsg    = "native channels are not allowed in workflow code"
	chanAdvice = "use signals (flows.Signals) or flows.Go with flows.WaitGroup"
)

const idAdvice = "generate ids with flows.SideEffect (or in an activity)"

// randomConstructors build a source or generator: deterministic given a
// seed (their methods are reported).
var randomConstructors = map[string]bool{"New": true, "NewSource": true, "NewPCG": true, "NewChaCha8": true, "NewZipf": true}

// checkCall reports forbidden calls, and returns false for flows.SideEffect
// (its function argument runs once, outside replay: no checks there).
func (c checker) checkCall(call *ast.CallExpr) bool {
	pass := c.pass
	fn := calledFunc(pass, call.Fun)
	if fn == nil || fn.Pkg() == nil {
		return true
	}
	if isSideEffect(pass, call) {
		// The function whose value is recorded runs outside replay; the
		// Ctx argument is still workflow code. A literal held in a
		// variable is skipped through sideEffectFns.
		if len(call.Args) > 0 {
			c.checkExpr(call.Args[0])
		}
		return false
	}
	if msg := forbiddenFunc(fn); msg != "" {
		pass.Reportf(call.Pos(), "%s", msg)
	}
	// Check the arguments, not the callee (reported above, not as a value).
	for _, a := range call.Args {
		c.checkExpr(a)
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		c.checkExpr(sel.X)
	}
	return false
}

// forbiddenFunc returns the report for a function or method that breaks
// determinism, or "".
func forbiddenFunc(fn *types.Func) string {
	path, name := fn.Pkg().Path(), fn.Name()
	if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
		return forbiddenMethod(sig, name)
	}
	advice, ok := forbidden[path]
	switch {
	case !ok:
		return ""
	case advice == nil && randomConstructors[name]:
		return ""
	case advice == nil:
		return path + "." + name + " is not deterministic in workflow code: use flows.Rand (or flows.SideEffect)"
	case advice[name] != "":
		return path + "." + name + " is not deterministic in workflow code: " + advice[name]
	}
	return ""
}

// forbiddenMethod returns the report for a method: those of a random
// source (wherever it was created), the unordered iteration of a sync.Map,
// sync primitives and errgroup.
func forbiddenMethod(sig *types.Signature, name string) string {
	recv := types.Unalias(sig.Recv().Type())
	if p, ok := recv.(*types.Pointer); ok {
		recv = types.Unalias(p.Elem())
	}
	named, ok := recv.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	rp, rn := named.Obj().Pkg().Path(), named.Obj().Name()
	switch rp {
	case "math/rand", "math/rand/v2":
		if rn == "Rand" {
			return rp + ".Rand." + name + " is not deterministic in workflow code: use flows.Rand (or flows.SideEffect)"
		}
	case "sync":
		return syncMethod(rn, name)
	case "golang.org/x/sync/errgroup":
		if rn == "Group" && (name == "Go" || name == "TryGo") {
			return "errgroup.Group." + name + " starts a goroutine: use flows.Go with flows.WaitGroup in workflow code"
		}
	}
	return ""
}

// syncMethod reports the methods of sync types, wherever the value lives (a
// struct field is not a definition checkDefs sees).
func syncMethod(typ, name string) string {
	switch typ {
	case "Map":
		if name == "Range" {
			return "sync.Map.Range iterates in no fixed order: not deterministic in workflow code"
		}
	case "WaitGroup":
		return "sync.WaitGroup." + name + " in workflow code: use flows.WaitGroup (and flows.Go)"
	case "Mutex", "RWMutex", "Cond", "Once":
		return "sync." + typ + "." + name + " in workflow code: not needed (coroutines never run in parallel) and can block it"
	}
	return ""
}

// calledFunc returns the function a call expression calls, if static
// (generic instantiations included).
func calledFunc(pass *analysis.Pass, fun ast.Expr) *types.Func {
	switch f := fun.(type) {
	case *ast.IndexExpr:
		return calledFunc(pass, f.X)
	case *ast.IndexListExpr:
		return calledFunc(pass, f.X)
	case *ast.ParenExpr:
		return calledFunc(pass, f.X)
	case *ast.Ident:
		fn, _ := pass.TypesInfo.Uses[f].(*types.Func)
		return fn
	case *ast.SelectorExpr:
		fn, _ := pass.TypesInfo.Uses[f.Sel].(*types.Func)
		return fn
	}
	return nil
}
