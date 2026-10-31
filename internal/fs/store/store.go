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

// ErrReplicaLagging means the elected standby has not yet caught up with the
// last acknowledged HEAD, so it must not accept writes (RPO=0 failover).
var ErrReplicaLagging = errors.New("store: replica is behind the acknowledged head")

// Range selects a byte window of an object.
type Range struct {
	Offset, Length int64 // Length <= 0 means "to end of object"
}

// DeletePrefix drops every object under prefix, using the store's bulk
// capability when available and list+delete otherwise.
func DeletePrefix(ctx context.Context, s Store, prefix string) error {
	if pd, ok := s.(PrefixDeleter); ok {
		return pd.DeletePrefix(ctx, prefix)
	}
	next := ""
	for {
		objs, n, err := s.ListPrefix(ctx, prefix, next, 1000)
		if err != nil {
			return err
		}
		if len(objs) > 0 {
			keys := make([]string, len(objs))
			for i := range objs {
				keys[i] = objs[i].Key
			}
			if err := s.Delete(ctx, keys); err != nil {
				return err
			}
		}
		if n == "" || n == next {
			break
		}
		next = n
	}
	return nil
}

// ReconstructibleWriter stores state that the client can re-send after a
// crash (DAV activity/transaction fragments, staged bodies). Such objects do
// not need the fsync cost of committed revision data: losing them only makes
// an uncommitted session restart.
type ReconstructibleWriter interface {
	PutReconstructible(ctx context.Context, key string, data []byte) error
}

// ReconstructibleStreamWriter is the streaming form of ReconstructibleWriter.
type ReconstructibleStreamWriter interface {
	PutFromReconstructible(ctx context.Context, key string, r io.Reader, size int64) error
}

// PutReconstructible writes session state, using the fast path when the store
// offers one.
func PutReconstructible(ctx context.Context, s Store, key string, data []byte) error {
	if w, ok := s.(ReconstructibleWriter); ok {
		return w.PutReconstructible(ctx, key, data)
	}
	return s.Put(ctx, key, data)
}

// PutFromReconstructible is the streaming form of PutReconstructible.
func PutFromReconstructible(ctx context.Context, s Store, key string, r io.Reader, size int64) error {
	if w, ok := s.(ReconstructibleStreamWriter); ok {
		return w.PutFromReconstructible(ctx, key, r, size)
	}
	return s.PutFrom(ctx, key, r, size)
}

// ConditionalDeleter removes an object only if its current content matches
// etag. Lease release uses it so a stale holder cannot delete a lock that a
// new owner just acquired.
type ConditionalDeleter interface {
	DeleteIfMatch(ctx context.Context, key, etag string) error
}

// DeleteIfMatch removes key if its content hash equals etag. Stores without a
// native conditional delete fall back to a CAS-to-tombstone followed by a
// delete: the tombstone carries no timestamp, so a crashed deleter is treated
// as stale by the next lease waiter.
func DeleteIfMatch(ctx context.Context, s Store, key, etag string) error {
	if cd, ok := s.(ConditionalDeleter); ok {
		return cd.DeleteIfMatch(ctx, key, etag)
	}
	if _, err := s.CAS(ctx, key, etag, []byte("deleted")); err != nil {
		return err
	}
	return s.Delete(ctx, []string{key})
}

// PrefixDeleter is implemented by stores that can drop a whole key prefix in
// one operation. Session cleanup after a large commit would otherwise unlink
// one object per changed file (tens of thousands on a 30k-file commit).
type PrefixDeleter interface {
	DeletePrefix(ctx context.Context, prefix string) error
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

	// PutFrom stores exactly size bytes read from r. Used to stream large
	// uploads without buffering them.
	PutFrom(ctx context.Context, key string, r io.Reader, size int64) error

	// Get reads an object, or a byte range of it.
	Get(ctx context.Context, key string, rng *Range) ([]byte, error)

	// GetTo streams an object to w without materializing it. Used for file
	// texts stored as separate objects (large files).
	GetTo(ctx context.Context, key string, w io.Writer) (int64, error)

	// Exists reports whether key is present. Unlike Head it must not read the
	// object body (FileStore's Head hashes the file to build its ETag).
	Exists(ctx context.Context, key string) (bool, error)

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

// FilePutter is implemented by stores that can move a local file into place
// without an extra copy (same-filesystem link), used for large uploads.
type FilePutter interface {
	PutFile(ctx context.Context, key, path string) error
}

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

func (m *MemoryStore) PutFrom(_ context.Context, key string, r io.Reader, size int64) error {
	data, err := io.ReadAll(io.LimitReader(r, size))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("store: short body (%d/%d)", len(data), size)
	}
	return m.Put(context.Background(), key, data)
}

func (m *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok, nil
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

func (m *MemoryStore) DeleteIfMatch(_ context.Context, key, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.data[key]
	if !ok || ContentETag(cur) != etag {
		return ErrPreconditionFailed
	}
	delete(m.data, key)
	delete(m.etag, key)
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
