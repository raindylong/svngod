package fs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"svngod/internal/fs/store"
)

// Content-defined chunking for large file texts.
//
// A chunked node stores a manifest in NodeRev.Text: a sequence of
// (size, sha256) pairs. Each chunk is an immutable object under
// <repo>/chunks/<hex sha256>. Inserting or replacing bytes only changes the
// chunks around the edit; the rest of the file reuses its old chunk objects,
// across revisions and across files.
const (
	chunkMin     = 256 << 10 // do not cut below 256 KiB
	chunkMax     = 4 << 20   // hard cap, keeps one chunk in memory
	chunkMask    = (1 << 20) - 1
	chunkMagic   = "CHK1"
	chunkHashLen = sha256.Size
	chunkRefLen  = 4 + chunkHashLen
)

// ChunkThreshold is the text size above which a new revision is stored as
// content-defined chunks rather than one external object (or a delta chain).
const ChunkThreshold = 4 << 20

type chunkRef struct {
	Size uint32
	Hash [chunkHashLen]byte
}

// gearTable is a deterministic 64-bit table for the rolling hash. It is
// generated once, so chunk boundaries are identical across processes and
// restarts: dedupe must not depend on map iteration order or RNG state.
var gearTable = func() [256]uint64 {
	var t [256]uint64
	x := uint64(0x9e3779b97f4a7c15)
	for i := 0; i < 256; i++ {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		t[i] = z ^ (z >> 31)
	}
	return t
}()

// splitChunks reads rd and calls emit for every content-defined chunk. The
// slice passed to emit is only valid for the duration of the call (the caller
// must consume or copy it); at most chunkMax bytes are held at a time.
func splitChunks(rd io.Reader, emit func(data []byte) error) error {
	br := bufio.NewReaderSize(rd, 1<<20)
	buf := make([]byte, 0, chunkMax)
	var h uint64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := emit(buf); err != nil {
			return err
		}
		buf = buf[:0]
		h = 0
		return nil
	}
	for {
		c, err := br.ReadByte()
		if err == io.EOF {
			return flush()
		}
		if err != nil {
			return err
		}
		buf = append(buf, c)
		h = (h << 1) + gearTable[c]
		if len(buf) >= chunkMax || (len(buf) >= chunkMin && h&chunkMask == 0) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func marshalChunkManifest(refs []chunkRef) []byte {
	b := make([]byte, 0, len(chunkMagic)+binary.MaxVarintLen64+len(refs)*chunkRefLen)
	b = append(b, chunkMagic...)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(refs)))
	b = append(b, tmp[:n]...)
	for _, ref := range refs {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], ref.Size)
		b = append(b, size[:]...)
		b = append(b, ref.Hash[:]...)
	}
	return b
}

func parseChunkManifest(b []byte) ([]chunkRef, error) {
	if len(b) < len(chunkMagic) || string(b[:len(chunkMagic)]) != chunkMagic {
		return nil, errors.New("fs: bad chunk manifest magic")
	}
	p := len(chunkMagic)
	count, n := binary.Uvarint(b[p:])
	if n <= 0 {
		return nil, errors.New("fs: bad chunk manifest count")
	}
	p += n
	if count > uint64(len(b))/chunkRefLen+1 {
		return nil, errors.New("fs: chunk manifest count out of range")
	}
	refs := make([]chunkRef, 0, count)
	for i := uint64(0); i < count; i++ {
		if p+chunkRefLen > len(b) {
			return nil, errors.New("fs: chunk manifest truncated")
		}
		ref := chunkRef{Size: binary.BigEndian.Uint32(b[p : p+4])}
		copy(ref.Hash[:], b[p+4:p+chunkRefLen])
		refs = append(refs, ref)
		p += chunkRefLen
	}
	if p != len(b) {
		return nil, errors.New("fs: chunk manifest trailing bytes")
	}
	return refs, nil
}

func chunkKeyHash(ref chunkRef) string {
	return fmt.Sprintf("%x", ref.Hash[:])
}

// manifestSize returns the total text length described by a manifest.
func manifestSize(refs []chunkRef) uint64 {
	var n uint64
	for _, r := range refs {
		n += uint64(r.Size)
	}
	return n
}

// chunkReader streams a chunked node's text in order, verifying each chunk's
// size and hash.
func (r *Repo) chunkReader(refs []chunkRef) io.Reader {
	return &chunkStream{repo: r, refs: refs}
}

type chunkStream struct {
	repo *Repo
	refs []chunkRef
	idx  int
	buf  bytes.Buffer
}

func (c *chunkStream) Read(p []byte) (int, error) {
	for c.buf.Len() == 0 {
		if c.idx >= len(c.refs) {
			return 0, io.EOF
		}
		ref := c.refs[c.idx]
		c.idx++
		b, err := c.repo.store.Get(context.Background(), c.repo.keys.Chunk(chunkKeyHash(ref)), nil)
		if err != nil {
			return 0, err
		}
		if uint32(len(b)) != ref.Size {
			return 0, fmt.Errorf("fs: chunk %s size %d, want %d", chunkKeyHash(ref), len(b), ref.Size)
		}
		if sum := sha256.Sum256(b); sum != ref.Hash {
			return 0, fmt.Errorf("fs: chunk %s checksum mismatch", chunkKeyHash(ref))
		}
		c.buf.Write(b)
	}
	return c.buf.Read(p)
}

// storeChunkedReader splits rd into content-defined chunks, stores the ones
// the repository does not have yet, and returns the manifest plus total size.
func (r *Repo) storeChunkedReader(ctx context.Context, rd io.Reader) ([]byte, int64, error) {
	// Chunk writes run on a small worker pool: hashing stays sequential (the
	// boundary decision depends on the byte stream) but the per-chunk fsyncs
	// overlap. At most workers*ChunkMax bytes are queued.
	const workers = 8
	type job struct {
		key  string
		data []byte
	}
	jobs := make(chan job, workers)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := r.store.Put(ctx, j.key, j.data); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	var refs []chunkRef
	var total int64
	err := splitChunks(rd, func(data []byte) error {
		sum := sha256.Sum256(data)
		ref := chunkRef{Size: uint32(len(data)), Hash: sum}
		key := r.keys.Chunk(chunkKeyHash(ref))
		refs = append(refs, ref)
		total += int64(len(data))
		ok, eerr := r.store.Exists(ctx, key)
		if eerr != nil {
			return eerr
		}
		if ok {
			return nil
		}
		cp := append([]byte(nil), data...)
		select {
		case jobs <- job{key: key, data: cp}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(jobs)
	wg.Wait()
	if err != nil {
		return nil, 0, err
	}
	if firstErr != nil {
		return nil, 0, firstErr
	}
	return marshalChunkManifest(refs), total, nil
}

// StoreChunkedReader splits and stores a large text from rd, returning the
// manifest and total size. Callers use it when the body is already on disk.
func (r *Repo) StoreChunkedReader(rd io.Reader) ([]byte, int64, error) {
	return r.storeChunkedReader(context.Background(), rd)
}

// readChunkedRange returns up to length bytes at off from a chunked text,
// reading only the chunks (or chunk sub-ranges) that overlap the window.
func (r *Repo) readChunkedRange(refs []chunkRef, off, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, errors.New("fs: range out of bounds")
	}
	total := int64(manifestSize(refs))
	if off > total {
		return nil, errors.New("fs: range out of bounds")
	}
	end := off + length
	if length <= 0 || end > total {
		end = total
	}
	out := make([]byte, 0, end-off)
	var pos int64
	for _, ref := range refs {
		cs := int64(ref.Size)
		cend := pos + cs
		if cend <= off {
			pos = cend
			continue
		}
		if pos >= end {
			break
		}
		lo, hi := int64(0), cs
		if off > pos {
			lo = off - pos
		}
		if end < cend {
			hi = end - pos
		}
		b, err := r.store.Get(context.Background(), r.keys.Chunk(chunkKeyHash(ref)), &store.Range{Offset: lo, Length: hi - lo})
		if err != nil {
			return nil, err
		}
		if int64(len(b)) != hi-lo {
			return nil, errors.New("fs: short chunk range")
		}
		out = append(out, b...)
		pos = cend
	}
	return out, nil
}
