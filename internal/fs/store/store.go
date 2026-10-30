// Package store defines the object-storage abstraction the repository FS
// layer depends on, plus an in-memory implementation used for tests and an
// S3 (MinIO/AWS/OSS) adapter.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// ErrNotFound is returned for reads of a missing object.
var ErrNotFound = errors.New("store: object not found")

// ErrPreconditionFailed is returned by CAS / CreateIfAbsent when the
// expected condition does not hold.
var ErrPreconditionFailed = errors.New("store: precondition failed")

// Range selects a byte window of an object.
type Range struct {
	Offset, Length int64 // Length <= 0 means "to end of object"
}

// ListedObj is one object returned by ListPrefix.
type ListedObj struct {
	Key  string
	Size int64
}

// Store is the object-storage abstraction. All objects are treated as
// immutable except the ones explicitly managed through CAS / Delete.
type Store interface {
	// Put creates a new immutable object. Callers guarantee the key is unique.
	Put(ctx context.Context, key string, data []byte) error

	// Get reads an object, or a byte range of it.
	Get(ctx context.Context, key string, rng *Range) ([]byte, error)

	// GetTo streams an object to w without materializing it. Used for file
	// texts stored as separate objects (large files).
	GetTo(ctx context.Context, key string, w io.Writer) (int64, error)

	// Head returns the object's ETag and existence.
	Head(ctx context.Context, key string) (etag string, exists bool, err error)

	// CreateIfAbsent creates key only if it does not already exist
	// (used for the commit lock). Fails with ErrPreconditionFailed otherwise.
	CreateIfAbsent(ctx context.Context, key string, data []byte) error

	// CAS atomically overwrites key only if its current ETag equals expect,
	// returning the new ETag. Fails with ErrPreconditionFailed on mismatch.
	CAS(ctx context.Context, key string, expectETag string, data []byte) (newETag string, err error)

	// ListPrefix lists objects under prefix, paged.
	ListPrefix(ctx context.Context, prefix, continuation string, max int) (objs []ListedObj, next string, err error)

	// Delete removes objects best-effort (GC only).
	Delete(ctx context.Context, keys []string) error
}

// ---------------------------------------------------------------------------

// MemoryStore is an in-memory Store used for unit tests.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	etag map[string]string
	seq  uint64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[string][]byte{}, etag: map[string]string{}}
}

func (m *MemoryStore) nextETag(key string) string {
	m.seq++
	return fmt.Sprintf("%q-%d", key, m.seq)
}

func (m *MemoryStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etag[key] = m.nextETag(key)
	return nil
}

func (m *MemoryStore) Get(_ context.Context, key string, rng *Range) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	if rng == nil {
		cp := make([]byte, len(data))
		copy(cp, data)
		return cp, nil
	}
	off := rng.Offset
	if off < 0 || off >= int64(len(data)) {
		return nil, fmt.Errorf("store: range offset %d out of bounds (%d)", off, len(data))
	}
	end := int64(len(data))
	if rng.Length > 0 && off+rng.Length < end {
		end = off + rng.Length
	}
	cp := make([]byte, end-off)
	copy(cp, data[off:end])
	return cp, nil
}

func (m *MemoryStore) GetTo(_ context.Context, key string, w io.Writer) (int64, error) {
	m.mu.RLock()
	data, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return 0, ErrNotFound
	}
	n, err := w.Write(data)
	return int64(n), err
}

func (m *MemoryStore) Head(_ context.Context, key string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.data[key]
	if !ok {
		return "", false, nil
	}
	return ContentETag(data), true, nil
}

func (m *MemoryStore) CreateIfAbsent(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return ErrPreconditionFailed
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etag[key] = ContentETag(data)
	return nil
}

func (m *MemoryStore) CAS(_ context.Context, key string, expectETag string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.data[key]
	if !ok || ContentETag(cur) != expectETag {
		return "", ErrPreconditionFailed
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	m.etag[key] = ContentETag(data)
	return m.etag[key], nil
}

func (m *MemoryStore) ListPrefix(_ context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if continuation != "" {
		for len(keys) > 0 && keys[0] <= continuation {
			keys = keys[1:]
		}
	}
	if max > 0 && len(keys) > max {
		next := keys[max-1]
		keys = keys[:max]
		return m.toListed(keys), next, nil
	}
	return m.toListed(keys), "", nil
}

func (m *MemoryStore) toListed(keys []string) []ListedObj {
	out := make([]ListedObj, 0, len(keys))
	for _, k := range keys {
		out = append(out, ListedObj{Key: k, Size: int64(len(m.data[k]))})
	}
	return out
}

func (m *MemoryStore) Delete(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.data, k)
		delete(m.etag, k)
	}
	return nil
}

// Keys returns all stored keys (test helper).
func (m *MemoryStore) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for k := range m.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
