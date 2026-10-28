// Package cache provides a simple concurrent LRU used to absorb S3 latency
// on hot rev-block / index / reconstructed-text reads.
package cache

import (
	"container/list"
	"sync"
)

// Key identifies a cache entry.
type Key string

// LRU is a fixed-capacity, concurrency-safe LRU cache keyed by strings.
type LRU struct {
	mu      sync.Mutex
	cap     int
	ll      *list.List
	items   map[Key]*list.Element
	evictFn func(Key, []byte)
}

type entry struct {
	key   Key
	value []byte
}

// New creates an LRU with the given capacity (0 disables caching).
func New(capacity int) *LRU {
	if capacity < 0 {
		capacity = 0
	}
	return &LRU{cap: capacity, ll: list.New(), items: map[Key]*list.Element{}}
}

// SetEvictFn registers a callback invoked when an entry is evicted.
func (l *LRU) SetEvictFn(f func(Key, []byte)) { l.evictFn = f }

// Get returns a cached value (copy) and whether it was present.
func (l *LRU) Get(k Key) ([]byte, bool) {
	if l == nil || l.cap == 0 {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.items[k]
	if !ok {
		return nil, false
	}
	l.ll.MoveToFront(el)
	v := el.Value.(*entry).value
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Put inserts or refreshes a value. When over capacity the least-recently-used
// entry is evicted.
func (l *LRU) Put(k Key, value []byte) {
	if l == nil || l.cap == 0 {
		return
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[k]; ok {
		el.Value.(*entry).value = cp
		l.ll.MoveToFront(el)
		return
	}
	el := l.ll.PushFront(&entry{key: k, value: cp})
	l.items[k] = el
	for l.ll.Len() > l.cap {
		old := l.ll.Back()
		e := old.Value.(*entry)
		l.ll.Remove(old)
		delete(l.items, e.key)
		if l.evictFn != nil {
			l.evictFn(e.key, e.value)
		}
	}
}

// Len returns the number of cached entries.
func (l *LRU) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ll.Len()
}

// Clear empties the cache.
func (l *LRU) Clear() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ll.Init()
	l.items = map[Key]*list.Element{}
}
