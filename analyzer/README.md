# flowsvet

A static analyzer for go-flows workflow code. Workflows are replayed from
their history, so their code must be deterministic. `flowsvet` reports, in
any function whose first parameter is `flows.Ctx`:

- `go` statements: use `flows.Go`;
- `select`, channel sends, receives and native channel types: use
  `flows.Select`, signals, `flows.Go` with `flows.WaitGroup`;
- ranging over a map: iterate over sorted keys;
- the wall clock (`time.Now`, `Sleep`, `After`, `NewTimer`, ...): use
  `flows.Now`, `flows.Sleep`, `flows.NewTimer`;
- randomness (`math/rand`, `crypto/rand`): use `flows.Rand`;
- `os.Getenv`: pass configuration as input;
- `sync.WaitGroup`, `sync.Mutex` and friends: use `flows.WaitGroup`.

Function literals passed to `flows.SideEffect` are not checked (their value
is recorded): pass the literal inline, as a function held in a variable is
checked. Activities are not checked (they take a `context.Context`).
Methods of a `math/rand` `Rand` and `sync.Map.Range` are reported too, and
so are forbidden functions used as values (`now := time.Now`).

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
