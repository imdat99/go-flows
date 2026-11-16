package flows

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/go-flows/internal/store/mem"
)

// keyLog records the keys an activity saw, by instance id.
type keyLog struct {
	mu   sync.Mutex
	seen map[string][]Key
}

func (l *keyLog) add(id string, k Key) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil {
		l.seen = map[string][]Key{}
	}
	l.seen[id] = append(l.seen[id], k)
}

// reset forgets id's keys: a repeated test run (-count) starts afresh.
func (l *keyLog) reset(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.seen, id)
}

func (l *keyLog) get(id string) []Key {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.seen[id])
}

var (
	keyRuns    keyLog
	keyComps   keyLog
	errKeyOnce = errors.New("first attempt fails")

	// keyStep fails its first attempt of each execution, so every
	// execution's key is seen twice (two attempts).
	keyStep = DefineActivity("test.key-step", func(_ context.Context, k Key, id string) (Key, struct{}, error) {
		keyRuns.add(id, k)
		if n := len(keyRuns.get(id)); n%2 == 1 {
			return "", struct{}{}, errKeyOnce
		}
		return k, struct{}{}, nil
	}, nil, WithRetry(Exponential(3, time.Millisecond, time.Millisecond)))

	keyChainWF = DefineWorkflow("test.key-chain", 1, func(c Ctx, in keyChain) ([]Key, error) {
		k, err := keyStep.Run(c, in.ID)
		if err != nil {
			return nil, err
		}
		in.Keys = append(in.Keys, k)
		if len(in.Keys) < 3 {
			return nil, ContinueAsNew(c, in)
		}
		return in.Keys, nil
	})

	keyUndo = DefineActivity("test.key-undo", func(_ context.Context, k Key, id string) (string, string, error) {
		keyRuns.add(id, k)
		return "", id, nil
	}, func(_ context.Context, k Key, id string) error {
		keyComps.add(id, k)
		return nil
	})

	keyUndoWF = DefineWorkflow("test.key-undo", 1, func(c Ctx, id string) (string, error) {
		if _, err := keyUndo.Run(c, id); err != nil {
			return "", err
		}
		return "", Permanent(errors.New("undo it"))
	})
)

type keyChain struct {
	ID   string `json:"id"`
	Keys []Key  `json:"keys"`
}

// Every execution of a continue-as-new chain gets its own activity key
// (schedule ids restart at 1 in each), stable across the attempts of one
// activity (T10.19).
func TestKeyUniquePerExecution(t *testing.T) {
	keyRuns.reset("key-chain-1")
	e := testEngine(t)
	keys, err := keyChainWF.Run(waitCtx(t), e, "key-chain-1", keyChain{ID: "key-chain-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("keys %q", keys)
	}
	if keys[0] == keys[1] || keys[1] == keys[2] || keys[0] == keys[2] {
		t.Fatalf("executions of a continue-as-new chain share a key: %q", keys)
	}
	seen := keyRuns.get("key-chain-1")
	if len(seen) != 6 {
		t.Fatalf("attempts saw %q", seen)
	}
	for i := 0; i < 6; i += 2 {
		if seen[i] != seen[i+1] || seen[i+1] != keys[i/2] {
			t.Fatalf("attempts of one activity saw different keys: %q (results %q)", seen, keys)
		}
	}
}

// An instance id reused after retention purged its ended execution gets
// new activity keys (T10.19).
func TestKeyUniqueAfterPurge(t *testing.T) {
	st := mem.New(nil)
	e, err := newEngine(st, Options{InstanceID: "test", PollMax: 20 * time.Millisecond, LeaseTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(bg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Stop(bg) })
	const id = "key-reuse-1"
	keyRuns.reset(id)
	first, err := keyChainWF.Run(waitCtx(t), e, id, keyChain{ID: id, Keys: []Key{"x", "y"}})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := st.PurgeEnded(bg, 0, 100); err != nil || n == 0 {
		t.Fatalf("purge: %d %v", n, err)
	}
	second, err := keyChainWF.Run(waitCtx(t), e, id, keyChain{ID: id, Keys: []Key{"x", "y"}})
	if err != nil {
		t.Fatal(err)
	}
	if first[2] == second[2] {
		t.Fatalf("a reused instance id repeats the key %q", first[2])
	}
}

// A compensator gets the key of the activity it undoes.
func TestKeyOfCompensator(t *testing.T) {
	e := testEngine(t)
	const id = "key-undo-1"
	keyRuns.reset(id)
	keyComps.reset(id)
	if _, err := keyUndoWF.Run(waitCtx(t), e, id, id); !errors.Is(err, ErrCompensated) {
		t.Fatalf("got %v", err)
	}
	runs, comps := keyRuns.get(id), keyComps.get(id)
	if len(runs) != 1 || len(comps) != 1 || runs[0] != comps[0] {
		t.Fatalf("invoked with %q, compensated with %q", runs, comps)
	}
}
