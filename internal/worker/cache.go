package worker

import (
	"container/list"
	"sync"

	"github.com/imdat99/go-flows/internal/executor"
)

// cache keeps the executors of recently run executions (the sticky cache),
// so that the next task of an execution does not replay its history.
//
// Executors are checked out while a task runs (take) and put back after a
// successful completion (put): an executor is never used by two tasks at
// once, even when a lease was lost and the same worker claims the execution
// again while the old task is still finishing (GW-9: go-workflows shared one
// cached executor between such tasks). After any error the task closes its
// executor instead of putting it back.
type cache struct {
	mu    sync.Mutex
	max   int
	items map[int64]*list.Element // execution id -> element of lru
	lru   list.List               // front: most recently used; values *executor.Executor
}

func newCache(size int) *cache {
	return &cache{max: size, items: make(map[int64]*list.Element)}
}

// take removes and returns the cached executor of an execution if its
// history ends at lastSeq; a stale one is closed.
func (c *cache) take(executionID, lastSeq int64) *executor.Executor {
	c.mu.Lock()
	el, ok := c.items[executionID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.items, executionID)
	c.lru.Remove(el)
	c.mu.Unlock()
	ex := exOf(el)
	if ex.LastSeq() != lastSeq {
		ex.Close() // another owner (or a lost lease) moved the history on
		return nil
	}
	return ex
}

// put caches ex, evicting the least recently used executors beyond max.
// An executor already cached for the execution (a newer one) wins.
func (c *cache) put(ex *executor.Executor) {
	if c.max <= 0 {
		ex.Close()
		return
	}
	var evicted []*executor.Executor
	c.mu.Lock()
	if _, ok := c.items[ex.ExecutionID()]; ok {
		c.mu.Unlock()
		ex.Close()
		return
	}
	c.items[ex.ExecutionID()] = c.lru.PushFront(ex)
	for c.lru.Len() > c.max {
		el := c.lru.Back()
		old := exOf(el)
		c.lru.Remove(el)
		delete(c.items, old.ExecutionID())
		evicted = append(evicted, old)
	}
	c.mu.Unlock()
	for _, old := range evicted {
		old.Close()
	}
}

// close closes every cached executor.
func (c *cache) close() {
	c.mu.Lock()
	var all []*executor.Executor
	for el := c.lru.Front(); el != nil; el = el.Next() {
		all = append(all, exOf(el))
	}
	c.items = make(map[int64]*list.Element)
	c.lru.Init()
	c.mu.Unlock()
	for _, ex := range all {
		ex.Close()
	}
}

// exOf returns the executor of a list element (the list only holds
// executors).
func exOf(el *list.Element) *executor.Executor {
	ex, _ := el.Value.(*executor.Executor)
	return ex
}
