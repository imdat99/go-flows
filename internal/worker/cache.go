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
//
// The cache is bounded by count (max) and by the executors' estimated
// bytes (maxBytes, executor.Size); beyond either, the least recently used
// executors are evicted and closed.
type cache struct {
	mu       sync.Mutex
	max      int
	maxBytes int64               // <= 0: no byte bound
	bytes    int64               // sum of the sizes of the cached executors
	items    map[int64]cacheItem // execution id -> its entry
	lru      list.List           // front: most recently used; values *executor.Executor
}

// cacheItem is a cached executor's list element and its size when it was
// put (an executor does not change while cached; keeping the size makes
// the byte count exact whatever Size would return later).
type cacheItem struct {
	el   *list.Element
	size int64
}

func newCache(size int, maxBytes int64) *cache {
	return &cache{max: size, maxBytes: maxBytes, items: make(map[int64]cacheItem)}
}

// take removes and returns the cached executor of an execution if its
// history ends at lastSeq; a stale one is closed.
func (c *cache) take(executionID, lastSeq int64) *executor.Executor {
	c.mu.Lock()
	it, ok := c.items[executionID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.items, executionID)
	c.lru.Remove(it.el)
	c.bytes -= it.size
	c.mu.Unlock()
	ex := exOf(it.el)
	if ex.LastSeq() != lastSeq {
		ex.Close() // another owner (or a lost lease) moved the history on
		return nil
	}
	return ex
}

// put caches ex, evicting the least recently used executors beyond max or
// maxBytes. An executor larger than maxBytes is not cached (and evicts
// nothing). An executor already cached for the execution (a newer one)
// wins.
func (c *cache) put(ex *executor.Executor) {
	if c.max <= 0 {
		ex.Close()
		return
	}
	size := ex.Size()
	if c.maxBytes > 0 && size > c.maxBytes {
		// Larger than the whole cache: dropped at once, rather than first
		// evicting every other executor (and again at each of its tasks).
		c.dropOversize(ex)
		return
	}
	var replaced *executor.Executor
	c.mu.Lock()
	if it, ok := c.items[ex.ExecutionID()]; ok {
		// Two drivers of one execution (a lease lost and taken over): keep
		// the executor further along its history; the other would only be
		// replayed past (a stale one is never used: take checks LastSeq).
		cur := exOf(it.el)
		if cur.LastSeq() >= ex.LastSeq() {
			c.mu.Unlock()
			ex.Close()
			return
		}
		replaced = cur
		it.el.Value = ex
		c.lru.MoveToFront(it.el)
		c.bytes += size - it.size
		it.size = size
		c.items[ex.ExecutionID()] = it
	} else {
		c.items[ex.ExecutionID()] = cacheItem{el: c.lru.PushFront(ex), size: size}
		c.bytes += size
	}
	var evicted []*executor.Executor
	for c.lru.Len() > c.max || (c.maxBytes > 0 && c.bytes > c.maxBytes) {
		el := c.lru.Back()
		old := exOf(el)
		c.lru.Remove(el)
		c.bytes -= c.items[old.ExecutionID()].size
		delete(c.items, old.ExecutionID())
		evicted = append(evicted, old)
	}
	c.mu.Unlock()
	if replaced != nil {
		replaced.Close()
	}
	for _, old := range evicted {
		old.Close()
	}
}

// dropOversize closes ex, an executor too large to cache, and the cached
// executor of its execution if ex is further along (that one is stale).
func (c *cache) dropOversize(ex *executor.Executor) {
	var stale *executor.Executor
	c.mu.Lock()
	if it, ok := c.items[ex.ExecutionID()]; ok {
		if cur := exOf(it.el); cur.LastSeq() < ex.LastSeq() {
			stale = cur
			c.lru.Remove(it.el)
			c.bytes -= it.size
			delete(c.items, ex.ExecutionID())
		}
	}
	c.mu.Unlock()
	ex.Close()
	if stale != nil {
		stale.Close()
	}
}

// close closes every cached executor.
func (c *cache) close() {
	c.mu.Lock()
	var all []*executor.Executor
	for el := c.lru.Front(); el != nil; el = el.Next() {
		all = append(all, exOf(el))
	}
	c.items = make(map[int64]cacheItem)
	c.lru.Init()
	c.bytes = 0
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
