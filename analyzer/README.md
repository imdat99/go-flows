# flowsvet

A static analyzer for go-flows workflow code. Workflows are replayed from
their history, so their code must be deterministic. `flowsvet` reports, in
any function whose first parameter is `flows.Ctx`:

- `go` statements: use `flows.Go`;
- `select`, channel sends, receives, native channel variables and every
  `make(chan ...)`, even one passed straight to a call: use `flows.Select`,
  signals, `flows.Go` with `flows.WaitGroup`;
- variables of a struct type that holds a channel or a sync primitive in a
  field (nested structs and arrays included, not behind pointers; only
  structs declared in the checked package or anonymous ones);
- ranging over a map: iterate over sorted keys;
- the wall clock (`time.Now`, `Sleep`, `After`, `NewTimer`, ...): use
  `flows.Now`, `flows.Sleep`, `flows.NewTimer`;
- randomness (`math/rand`, `crypto/rand`): use `flows.Rand`;
- `os.Getenv`, `Environ`, `Hostname`, `Getpid`, `Getwd`: pass configuration
  as input;
- `context.WithTimeout` and `WithDeadline` (wall-clock deadlines): use
  `flows.NewTimer` with `flows.Select`;
- random ids from `github.com/google/uuid` (`New`, `NewString`, `NewV7`...;
  not the name-based `NewSHA1`/`NewMD5`): use `flows.SideEffect`;
- `sync.WaitGroup`, `sync.Mutex` and friends, declared or reached through a
  struct field, and `errgroup.Group.Go`: use `flows.Go` and
  `flows.WaitGroup`.

Function literals passed to `flows.SideEffect` are not checked (their value
is recorded), inline or held in a local variable whose only uses are as the
argument of `flows.SideEffect` (a variable also called or passed elsewhere
keeps its literals checked). Activities are not checked (they take a `context.Context`).
Methods of a `math/rand` `Rand` and `sync.Map.Range` are reported too (the
constructors `rand.New`, `NewSource`, `NewPCG`... are not), and so are
forbidden functions used as values (`now := time.Now`).

Only functions taking `flows.Ctx` are checked: a helper without it that
iterates over a map, for example, is not.

It is a separate module, so its dependency on `golang.org/x/tools` stays out
of go-flows itself.

## go vet

```sh
go install github.com/imdat99/go-flows/analyzer/cmd/flowsvet@latest
go vet -vettool=$(which flowsvet) ./...
```

## golangci-lint (module plugin)

`.custom-gcl.yml`:

```yaml
version: v2.15.0
plugins:
  - module: github.com/imdat99/go-flows/analyzer
    version: latest
```

`.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - flowsvet
  settings:
    custom:
      flowsvet:
        type: module
```

Ported from the go-workflows analyzer (MIT, see `NOTICE`).
