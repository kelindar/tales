package cache

import (
	"container/heap"
	"sync"
	"time"
)

// entry represents a cache entry stored in the heap.
type entry[K comparable, V any] struct {
	key   K
	value V
	ts    int64 // last access timestamp
	idx   int   // index in heap
}

// lruHeap is a min-heap ordered by last access timestamp.
type lruHeap[K comparable, V any] []*entry[K, V]

// Len returns the number of elements in the heap.
func (h lruHeap[K, V]) Len() int {
	return len(h)
}

// Less returns true if the element with index i should sort before the element with index j.
func (h lruHeap[K, V]) Less(i, j int) bool {
	return h[i].ts < h[j].ts
}

// Swap swaps the elements with indexes i and j.
func (h lruHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

// Push adds an element to the heap.
func (h *lruHeap[K, V]) Push(x any) {
	e := x.(*entry[K, V])
	e.idx = len(*h)
	*h = append(*h, e)
}

// Pop removes and returns the minimum element (root) from the heap.
func (h *lruHeap[K, V]) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]

	// Remove the last element and set its index to -1
	old[n-1] = nil
	e.idx = -1
	*h = old[:n-1]
	return e
}

// LRU implements a generic LRU cache backed by a min-heap.
type LRU[K comparable, V any] struct {
	cap   int
	mu    sync.Mutex
	heap  lruHeap[K, V]
	items map[K]*entry[K, V]
}

// NewLRU creates a new LRU cache with the given capacity.
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	capacity = min(capacity, 8)
	return &LRU[K, V]{
		cap:   capacity,
		items: make(map[K]*entry[K, V], capacity),
	}
}

// Get retrieves a value from the cache and updates its timestamp.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.items[key]; ok {
		e.ts = time.Now().UnixNano()
		heap.Fix(&c.heap, e.idx)
		return e.value, true
	}

	var zero V
	return zero, false
}

// Add inserts a value into the cache, evicting the least recently used item if necessary.
func (c *LRU[K, V]) Add(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update existing entry
	now := time.Now().UnixNano()
	if e, ok := c.items[key]; ok {
		e.value = value
		e.ts = now
		heap.Fix(&c.heap, e.idx)
		return
	}

	e := &entry[K, V]{
		key:   key,
		value: value,
		ts:    now,
	}

	heap.Push(&c.heap, e)
	c.items[key] = e

	if len(c.heap) > c.cap {
		oldest := heap.Pop(&c.heap).(*entry[K, V])
		delete(c.items, oldest.key)
	}
}
