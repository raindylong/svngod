package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// Chunk storage for large file texts. A chunked node stores a manifest of
// (size, sha256) refs in NodeRev.Text; the chunks themselves are immutable
// objects under <repo>/chunks/<sha256>, shared across revisions and files.
// Chunk boundaries come from internal/cdc, which svngocli also uses.

func (r *Repo) chunkKey(ref cdc.Ref) string {
	return r.keys.Chunk(hex.EncodeToString(ref.Hash[:]))
}

// storeChunkedReader splits rd into content-defined chunks, stores the ones
// the repository does not have yet, and returns the manifest plus total size.
func (r *Repo) storeChunkedReader(ctx context.Context, rd io.Reader) ([]byte, int64, error) {
	// Chunk writes run on a small worker pool: boundaries depend on the byte
	// stream, so splitting stays sequential while the per-chunk fsyncs
	// overlap. At most workers*Max bytes are queued.
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
				ok, err := r.store.Exists(ctx, j.key)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				if ok {
					continue
				}
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
	var refs []cdc.Ref
	var total int64
	err := cdc.SplitParams(r.chunkParams, rd, func(data []byte) error {
		sum := sha256.Sum256(data)
		ref := cdc.Ref{Size: uint32(len(data)), Hash: sum}
		refs = append(refs, ref)
		total += int64(len(data))
		// Existence check and write both happen on the pool: on S3 a HEAD
		// costs a round trip and doing it inline serialised the whole upload.
		cp := append([]byte(nil), data...)
		select {
		case jobs <- job{key: r.chunkKey(ref), data: cp}:
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
	return cdc.Marshal(refs), total, nil
}

// StoreChunkedReader splits and stores a large text from rd, returning the
// manifest and total size. Callers use it when the body is already on disk.
func (r *Repo) StoreChunkedReader(rd io.Reader) ([]byte, int64, error) {
	return r.storeChunkedReader(context.Background(), rd)
}

// readChunkedRange returns up to length bytes at off from a chunked text,
// reading only the chunks (or chunk sub-ranges) that overlap the window.
func (r *Repo) readChunkedRange(refs []cdc.Ref, off, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, errors.New("fs: range out of bounds")
	}
	total := int64(cdc.Size(refs))
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
		b, err := r.store.Get(context.Background(), r.chunkKey(ref), &store.Range{Offset: lo, Length: hi - lo})
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

// chunkReader streams a chunked node's text in order, verifying each chunk's
// size and hash.
func (r *Repo) chunkReader(refs []cdc.Ref) io.Reader {
	return &chunkStream{repo: r, refs: refs}
}

type chunkStream struct {
	repo *Repo
	refs []cdc.Ref
	idx  int
	buf  bytes.Buffer
}

func chunkValid(b []byte, ref cdc.Ref) bool {
	return uint32(len(b)) == ref.Size && sha256.Sum256(b) == ref.Hash
}

// repairChunk asks a multi-copy store for a valid copy of a corrupt chunk and
// lets it overwrite the bad replicas (chunk keys are content addressed).
func (r *Repo) repairChunk(ref cdc.Ref) []byte {
	rg, ok := r.store.(store.RepairingGetter)
	if !ok {
		return nil
	}
	want := ref
	b, err := rg.GetRepairing(context.Background(), r.chunkKey(ref), func(cand []byte) bool {
		return chunkValid(cand, want)
	})
	if err != nil {
		return nil
	}
	return b
}

func (c *chunkStream) Read(p []byte) (int, error) {
	for c.buf.Len() == 0 {
		if c.idx >= len(c.refs) {
			return 0, io.EOF
		}
		ref := c.refs[c.idx]
		c.idx++
		b, err := c.repo.store.Get(context.Background(), c.repo.chunkKey(ref), nil)
		if err != nil {
			return 0, err
		}
		if !chunkValid(b, ref) {
			if fixed := c.repo.repairChunk(ref); fixed != nil {
				b = fixed
			}
		}
		if uint32(len(b)) != ref.Size {
			return 0, fmt.Errorf("fs: chunk %s size %d, want %d", cdc.HashHex(ref), len(b), ref.Size)
		}
		if sum := sha256.Sum256(b); sum != ref.Hash {
			return 0, fmt.Errorf("fs: chunk %s checksum mismatch", cdc.HashHex(ref))
		}
		c.buf.Write(b)
	}
	return c.buf.Read(p)
}

// WriteTextTo streams a file node's text to w without materialising it when the
// node stores it as content-defined chunks or an external object. Inline and
// delta texts are decoded in memory (delta chains are bounded by MAX_DELTA).
// It returns the number of bytes written.
func (r *Repo) WriteTextTo(nid NodeID, w io.Writer) (int64, error) {
	ctx := context.Background()
	nr, err := r.GetNodeRev(nid)
	if err != nil {
		return 0, err
	}
	switch nr.TextRep {
	case TextRepExternal:
		if nr.TextRef == "" {
			return 0, errors.New("fs: external text without a reference")
		}
		return r.store.GetTo(ctx, r.keys.Text(nr.TextRef), w)
	case TextRepChunked:
		refs, perr := cdc.Parse(nr.Text)
		if perr != nil {
			return 0, perr
		}
		var total int64
		for _, ref := range refs {
			n, gerr := r.store.GetTo(ctx, r.keys.Chunk(cdc.HashHex(ref)), w)
			if gerr != nil {
				return total, gerr
			}
			total += n
		}
		return total, nil
	default:
		data, gerr := r.GetText(nid)
		if gerr != nil {
			return 0, gerr
		}
		n, werr := w.Write(data)
		return int64(n), werr
	}
}
