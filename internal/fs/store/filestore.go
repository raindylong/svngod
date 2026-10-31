package store

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// FileStore implements Store on a local directory, one file per object.
// Object keys (slash-separated, from Keys) map to relative paths under base.
// Used by the svnadmin CLI when no S3 endpoint is configured.
type FileStore struct {
	base string
	mu   sync.Mutex
	// dirs caches directories already created; MkdirAll on an existing deep
	// path stats every component, which a 30k-file commit paid per fragment.
	dirs map[string]bool
}

// NewFileStore builds a FileStore rooted at base (created if missing).
func NewFileStore(base string) (*FileStore, error) {
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, err
	}
	return &FileStore{base: base, dirs: make(map[string]bool)}, nil
}

// objPath maps a slash-separated key to an absolute path, rejecting escapes.
func (s *FileStore) objPath(key string) (string, error) {
	if key == "" || strings.Contains(key, "\x00") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || filepath.IsAbs(clean) {
		return "", fmt.Errorf("store: key escapes base: %q", key)
	}
	return filepath.Join(s.base, clean), nil
}

// ensureDirLocked creates dir once. Caller holds s.mu.
func (s *FileStore) ensureDirLocked(dir string) error {
	if s.dirs[dir] {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if len(s.dirs) < 1<<16 {
		s.dirs[dir] = true
	}
	return nil
}

func (s *FileStore) Put(_ context.Context, key string, data []byte) error {
	p, err := s.objPath(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Dir(p)
	if err := s.ensureDirLocked(dir); err != nil {
		return err
	}
	// Write-then-rename: a crash mid-write must not leave a truncated object
	// that a resumed session would parse as its staged change.
	tmp, err := os.CreateTemp(dir, ".put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *FileStore) Get(_ context.Context, key string, rng *Range) ([]byte, error) {
	p, err := s.objPath(key)
	if err != nil {
		return nil, err
	}
	if rng == nil {
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			if os.IsNotExist(rerr) {
				return nil, ErrNotFound
			}
			return nil, rerr
		}
		return data, nil
	}
	// Ranged reads are used to apply svndiff windows against multi-gigabyte
	// base texts: reading the whole object and slicing it would allocate the
	// full file for every window.
	f, oerr := os.Open(p)
	if oerr != nil {
		if os.IsNotExist(oerr) {
			return nil, ErrNotFound
		}
		return nil, oerr
	}
	defer f.Close()
	st, serr := f.Stat()
	if serr != nil {
		return nil, serr
	}
	size := st.Size()
	off := rng.Offset
	if off < 0 || off > size {
		return nil, fmt.Errorf("store: range offset %d out of bounds (len %d)", off, size)
	}
	end := size
	if rng.Length > 0 && off+rng.Length < end {
		end = off + rng.Length
	}
	buf := make([]byte, end-off)
	if _, rerr := io.ReadFull(io.NewSectionReader(f, off, end-off), buf); rerr != nil {
		return nil, rerr
	}
	return buf, nil
}

// PutFile links (or copies) a local file into the object at key. Callers use
// content-addressed keys, so an existing object is already the right bytes.
func (s *FileStore) PutFile(_ context.Context, key, src string) error {
	p, err := s.objPath(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDirLocked(filepath.Dir(p)); err != nil {
		return err
	}
	if err := os.Link(src, p); err == nil {
		return nil
	}
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	n, err := io.Copy(tmp, f)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		_ = n
		return err
	}
	return nil
}

// PutFrom streams into a temp file and renames it into place, so readers never
// observe a partially written object.
func (s *FileStore) PutFrom(_ context.Context, key string, r io.Reader, size int64) error {
	p, err := s.objPath(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	n, err := io.Copy(tmp, io.LimitReader(r, size+1))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("store: short body (%d/%d)", n, size)
	}
	return os.Rename(tmpName, p)
}

func (s *FileStore) Exists(_ context.Context, key string) (bool, error) {
	p, err := s.objPath(key)
	if err != nil {
		return false, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !fi.IsDir(), nil
}

func (s *FileStore) GetTo(_ context.Context, key string, w io.Writer) (int64, error) {
	p, err := s.objPath(key)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	defer f.Close()
	return io.Copy(w, f)
}

func (s *FileStore) Head(_ context.Context, key string) (string, bool, error) {
	p, err := s.objPath(key)
	if err != nil {
		return "", false, err
	}
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if fi.IsDir() {
		return "", false, os.ErrNotExist
	}
	// Content-hash ETag, matching headLocked/CAS below. Returning file size
	// here while CAS compares MD5 would make every commit's precondition fail.
	data, err := os.ReadFile(p)
	if err != nil {
		return "", false, err
	}
	return ContentETag(data), true, nil
}

func (s *FileStore) CreateIfAbsent(_ context.Context, key string, data []byte) error {
	p, err := s.objPath(key)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDirLocked(filepath.Dir(p)); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return ErrPreconditionFailed
		}
		return err
	}
	_, werr := f.Write(data)
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	return cerr
}

func (s *FileStore) CAS(_ context.Context, key string, expectETag string, data []byte) (string, error) {
	p, err := s.objPath(key)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok, err := s.headLocked(p)
	if err != nil {
		return "", err
	}
	if !ok || cur != expectETag {
		return "", ErrPreconditionFailed
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return "", err
	}
	return ContentETag(data), nil
}

func (s *FileStore) headLocked(p string) (string, bool, error) {
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	if fi.IsDir() {
		return "", false, os.ErrNotExist
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return "", false, err
	}
	return ContentETag(data), true, nil
}

// contentETag returns a content hash of data so that CAS on mutable objects
// (the current HEAD pointer) never collides across revisions that happen to
// have the same byte length. File size is a poor ETag: two revisions of
// "current" can have equal size and CAS would wrongly accept a stale writer.
func ContentETag(data []byte) string {
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

func (s *FileStore) ListPrefix(_ context.Context, prefix, continuation string, max int) ([]ListedObj, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	root := filepath.Join(s.base, filepath.FromSlash(prefix))
	var out []ListedObj
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip missing
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.base, path)
		if err != nil {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		out = append(out, ListedObj{Key: filepath.ToSlash(rel), Size: fi.Size()})
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	// continuation is the last key of the previous page (S3 StartAfter
	// semantics): resume strictly after it.
	if continuation != "" {
		i := sort.Search(len(out), func(i int) bool { return out[i].Key > continuation })
		out = out[i:]
	}
	if max > 0 && len(out) > max {
		return out[:max], out[max-1].Key, nil
	}
	return out, "", nil
}

// DeletePrefix removes a directory tree under base (one syscall per directory
// instead of one per object).
func (s *FileStore) DeletePrefix(_ context.Context, prefix string) error {
	if prefix == "" || strings.Contains(prefix, "\x00") {
		return fmt.Errorf("store: invalid prefix %q", prefix)
	}
	clean := filepath.Clean(filepath.FromSlash(prefix))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || filepath.IsAbs(clean) {
		return fmt.Errorf("store: prefix escapes base: %q", prefix)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.RemoveAll(filepath.Join(s.base, clean))
	if err != nil {
		return err
	}
	return nil
}

func (s *FileStore) Delete(_ context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		p, err := s.objPath(k)
		if err != nil {
			continue
		}
		_ = os.Remove(p)
	}
	return nil
}
