package dump

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"svngod/internal/fs"
)

// decodeProps parses the svn proplist format back into a map.
func decodeProps(data []byte) (map[string]string, error) {
	props := map[string]string{}
	r := bufio.NewReader(bytes.NewReader(data))
	for {
		line, err := r.ReadString('\n')
		if err != nil && line == "" {
			break
		}
		line = strings.TrimRight(line, "\n")
		if line == "PROPS-END" {
			break
		}
		if !strings.HasPrefix(line, "K ") {
			return nil, fmt.Errorf("dump: expected K header, got %q", line)
		}
		klen, err := strconv.Atoi(strings.TrimSpace(line[2:]))
		if err != nil {
			return nil, err
		}
		key := make([]byte, klen+1) // include trailing newline
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}
		keyStr := string(key[:klen])

		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\n")
		if !strings.HasPrefix(line, "V ") {
			return nil, fmt.Errorf("dump: expected V header, got %q", line)
		}
		vlen, err := strconv.Atoi(strings.TrimSpace(line[2:]))
		if err != nil {
			return nil, err
		}
		val := make([]byte, vlen+1)
		if _, err := io.ReadFull(r, val); err != nil {
			return nil, err
		}
		props[keyStr] = string(val[:vlen])
	}
	return props, nil
}

// Load reads an svn dump v2 stream and replays it into repo via CommitTxn.
func Load(repo *fs.Repo, r io.Reader) error {
	return LoadRange(repo, r, 0, 0)
}

// LoadRange loads revision numbers in [from,to] (0 = unbounded on that side).
// Revisions the target already has are skipped, so a range load can continue an
// interrupted import; `from` must not exceed the target's HEAD+1, otherwise the
// result would silently miss history.
func LoadRange(repo *fs.Repo, r io.Reader, from, to uint64) error {
	return LoadRangeProgress(repo, r, from, to, nil)
}

// LoadRangeProgress is LoadRange with a callback after each applied revision
// (skipped revisions do not call it).
func LoadRangeProgress(repo *fs.Repo, r io.Reader, from, to uint64, progress func(rev uint64)) error {
	// (uuid handling below: adopt the dump's UUID when loading into an empty
	// repository)
	br := bufio.NewReader(r)
	// header
	head, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(strings.TrimSpace(head), "SVN-fs-dump-format-version:") {
		return fmt.Errorf("dump: bad header %q", head)
	}

	var changes []fs.Change
	// Spooled bodies must not survive a failed load. liveSpool tracks every body
	// file that has been read but not yet committed, so a failure in the middle
	// of a buffered batch cleans up the whole batch, not just the block being
	// parsed.
	var liveSpool []string
	defer func() {
		for _, p := range liveSpool {
			os.Remove(p)
		}
	}()
	var props map[string]string
	inRevision := false
	var lastRev uint64
	startHead, _ := repo.Head()
	dumpUUID := ""
	// Resume support: a dump can be replayed after an interrupted load. Every
	// revision the target already has (number <= HEAD) is parsed but discarded,
	// and the remaining ones are appended on top of the existing HEAD. Loading
	// is per-revision atomic, so re-running the same dump continues where it
	// stopped instead of duplicating history.
	if from > 0 && from > startHead+1 {
		return fmt.Errorf("dump: --from %d but repository head is %d (history would be missing)", from, startHead)
	}
	lastRev = startHead
	skipping := false

	// Revisions are committed in batches: one commit lease, one WAL record and
	// one HEAD CAS per batch instead of per revision. On a WAN-backed store that
	// is the difference between ~0.7 and ~30 revisions/s (see
	// docs/性能基线与回归.md), which is what makes restoring a large repository
	// practical at all.
	//
	// Ordering is preserved: CommitBatch applies the requests consecutively to
	// the evolving tree, so revision N still sees revision N-1's content
	// (incremental import across shard boundaries depends on this).
	batchSize := loadBatchSize
	pending := make([]fs.BatchCommit, 0, batchSize)
	pendingChanges := make([][]fs.Change, 0, batchSize)

	// dropSpooled removes the body files of committed (or rejected) revisions
	// and forgets them, so the deferred sweep does not re-remove them.
	dropSpooled := func(chs []fs.Change) {
		for _, ch := range chs {
			if ch.DataFile == "" {
				continue
			}
			os.Remove(ch.DataFile)
			for i, p := range liveSpool {
				if p == ch.DataFile {
					liveSpool = append(liveSpool[:i], liveSpool[i+1:]...)
					break
				}
			}
		}
	}

	commitPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		revs, errs, err := repo.CommitBatch(pending)
		if err != nil {
			// Whole-batch failure: nothing was published, so every buffered
			// revision is lost. Surface the first per-request error when there is
			// one; it names the revision that actually failed.
			for i := range errs {
				if errs[i] != nil {
					err = errs[i]
					break
				}
			}
		}
		for _, chs := range pendingChanges {
			dropSpooled(chs)
		}
		pending = pending[:0]
		pendingChanges = pendingChanges[:0]
		if err != nil {
			return err
		}
		for _, rv := range revs {
			if rv == 0 {
				return fmt.Errorf("dump: batch commit returned an empty revision")
			}
			lastRev = rv
			if progress != nil {
				progress(rv)
			}
		}
		return nil
	}

	flush := func() error {
		if inRevision && !skipping && len(changes) > 0 {
			// A copy's source must already be published: applyChange loads the
			// source subtree from the store at CopyFromRev. If that revision is
			// still buffered in this batch it does not exist yet, so publish the
			// buffer first and let the copy land in the next batch.
			for _, ch := range changes {
				// CopyFromRev must already be published; lastRev is the highest
				// published revision, so anything above it is still buffered.
				if ch.CopyFromRev > lastRev {
					if err := commitPending(); err != nil {
						return err
					}
					break
				}
			}
			for _, ch := range changes {
				if ch.DataFile != "" {
					liveSpool = append(liveSpool, ch.DataFile)
				}
			}
			pending = append(pending, fs.BatchCommit{BaseRev: lastRev + uint64(len(pending)), Changes: changes, RevProps: props})
			pendingChanges = append(pendingChanges, changes)
			if len(pending) >= batchSize {
				if err := commitPending(); err != nil {
					return err
				}
			}
		}
		changes = nil
		return nil
	}

	// finish commits the tail and reports a failure the same way flush does.
	finish := func() error { return commitPending() }

	for {
		hdrs, err := readHeaders(br)
		if len(hdrs) == 0 {
			if err == io.EOF {
				if ferr := flush(); ferr != nil {
					return ferr
				}
				if ferr := finish(); ferr != nil {
					return ferr
				}
				if startHead == 0 && dumpUUID != "" {
					return repo.SetUUID(dumpUUID)
				}
				return nil
			}
			if err != nil {
				return err
			}
			continue // blank separator between blocks
		}
		if err != nil && err != io.EOF {
			return err
		}

		if rn, ok := hdrs["Revision-number"]; ok {
			if err := flush(); err != nil {
				return err
			}
			if n, perr := strconv.ParseUint(strings.TrimSpace(rn), 10, 64); perr == nil {
				if to > 0 && n > to {
					// Requested range complete: publish what is buffered before
					// returning, otherwise the last partial batch is silently lost.
					if ferr := finish(); ferr != nil {
						return ferr
					}
					return nil
				}
				skipping = n <= startHead || (from > 0 && n < from)
			} else {
				skipping = false
			}
			contentLen, err := strconv.Atoi(hdrs["Content-length"])
			if err != nil {
				return fmt.Errorf("dump: bad Content-length %q", hdrs["Content-length"])
			}
			plen, err := strconv.Atoi(hdrs["Prop-content-length"])
			if err != nil {
				return fmt.Errorf("dump: bad Prop-content-length")
			}
			if contentLen < plen {
				return fmt.Errorf("dump: Content-length %d < Prop-content-length %d", contentLen, plen)
			}
			propText := make([]byte, plen)
			if _, err := io.ReadFull(br, propText); err != nil {
				return err
			}
			props, err = decodeProps(propText)
			if err != nil {
				return err
			}
			// Node records follow the revision's property block and are NOT
			// counted in Content-length (standard svnadmin dumps); any extra
			// bytes in Content-length beyond the props are padding.
			if pad := contentLen - plen; pad > 0 {
				if _, err := io.CopyN(io.Discard, br, int64(pad)); err != nil {
					return err
				}
			}
			inRevision = true
			continue
		}

		// Preamble blocks such as "UUID: <uuid>" carry stream metadata, not
		// repository content; skip them instead of failing (standard dumps
		// always contain one).
		if u, ok := hdrs["UUID"]; ok && !inRevision {
			// Keep the source repository's identity; svnadmin load does the
			// same (a repository loaded into an empty repo adopts the UUID).
			dumpUUID = u
			continue
		}
		if _, ok := hdrs["Node-path"]; !ok {
			if !inRevision {
				continue // unknown preamble block
			}
			return fmt.Errorf("dump: unexpected headers %v", hdrs)
		}
		if !inRevision {
			return fmt.Errorf("dump: node before any revision")
		}
		if skipping {
			// Consume the node without materialising its text: a skipped 1 GiB
			// file used to allocate 1 GiB just to throw it away.
			if err := skipNodeContent(br, hdrs); err != nil {
				return err
			}
			continue
		}
		cs, _, err := buildChange(repo, br, hdrs)
		if err != nil {
			return err
		}
		changes = append(changes, cs...)
	}
}

// readHeaders reads header lines until a blank line, returning a map.
func readHeaders(br *bufio.Reader) (map[string]string, error) {
	hdrs := map[string]string{}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return hdrs, err
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			return hdrs, nil
		}
		if i := strings.Index(line, ":"); i >= 0 {
			hdrs[strings.TrimSpace(line[:i])] = strings.TrimSpace(line[i+1:])
		}
	}
}

// buildChange reads a node's props + text and turns it into fs.Change(s).
// A "replace" expands to delete-then-add so the commit preserves the new
// content instead of a bare delete.
// loadBatchSize is how many revisions LoadRangeProgress submits per CommitBatch
// call. One batch costs one commit lease, one WAL record and one HEAD CAS
// regardless of size, so batching is the main lever for restoring over a
// high-latency store. A variable so svnadmin can expose it.
// loadBatchSize is the default number of revisions per CommitBatch call.
//
// Measured on the two-instance S3 lab with a 10000-revision repository (3.4 MB
// dump): batch 64 -> 312 s, 256 -> 108 s, 512 -> 66 s, 1000 -> 43 s,
// 5000 -> 31 s; peak RSS was 53 / 69 / 101 MB for 256 / 1000 / 5000. Memory
// grows with the number of nodes in a batch, not with text size: texts above
// 4 MiB are stored as chunk manifests and bodies above 8 MiB are spooled to
// disk, so a large batch of ordinary revisions stays well under 100 MB.
//
// 1000 keeps the restore close to the knee (43 s for 10k revisions, ~233 rev/s)
// while leaving a wide margin; --batch N overrides it either way.
var loadBatchSize = 1000

// SetLoadBatchSize overrides the revisions per commit used by load (<=1 keeps
// the historical per-revision behaviour). Returns the previous value.
func SetLoadBatchSize(n int) int {
	prev := loadBatchSize
	if n < 1 {
		n = 1
	}
	loadBatchSize = n
	return prev
}

// loadSpoolThreshold is the node text size above which the body is spooled to a
// temp file instead of being buffered. A variable so tests can exercise the
// spooling path with small files.
var loadSpoolThreshold = int64(8 << 20)

func buildChange(repo *fs.Repo, br *bufio.Reader, hdrs map[string]string) ([]fs.Change, int, error) {
	// Dump node paths are repository-relative ("trunk/a.txt"); fs.Change
	// wants absolute svn paths ("/trunk/a.txt"). Older svngod dumps wrote a
	// leading slash, so normalize instead of blindly prefixing.
	path := "/" + strings.TrimPrefix(hdrs["Node-path"], "/")
	action := hdrs["Node-action"]
	kind := hdrs["Node-kind"]
	plen, _ := strconv.Atoi(hdrs["Prop-content-length"])
	tlen := 0
	if v, ok := hdrs["Text-content-length"]; ok {
		tlen, _ = strconv.Atoi(v)
	}

	propText := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(br, propText); err != nil {
			return nil, 0, err
		}
	}
	var content []byte
	var spooled string
	var md5sum [16]byte
	var sha1sum [20]byte
	hashed := false
	if tlen > 0 {
		if int64(tlen) > loadSpoolThreshold {
			// Large node bodies are spooled: a dump carrying a 2 GiB file must
			// not require 2 GiB of RAM to load. The name matches the server's
			// temp-file convention so a crash leaves a cleanable file.
			tmp, terr := os.CreateTemp("", fmt.Sprintf("svngod-txn-%d-*", os.Getpid()))
			if terr != nil {
				return nil, 0, terr
			}
			h := md5.New()
			hs := sha1.New()
			n, cerr := io.CopyN(io.MultiWriter(tmp, h, hs), br, int64(tlen))
			tmp.Close()
			if cerr != nil || n != int64(tlen) {
				os.Remove(tmp.Name())
				if cerr == nil {
					cerr = fmt.Errorf("dump: short text (%d of %d bytes)", n, tlen)
				}
				return nil, 0, cerr
			}
			spooled = tmp.Name()
			copy(md5sum[:], h.Sum(nil))
			copy(sha1sum[:], hs.Sum(nil))
			hashed = true
		} else {
			content = make([]byte, tlen)
			if _, err := io.ReadFull(br, content); err != nil {
				return nil, 0, err
			}
		}
	}

	c := fs.Change{Path: path, Data: content, DataFile: spooled}
	if hashed {
		c.DataMD5 = md5sum
		c.HasMD5 = true
		c.DataSHA1 = sha1sum
		c.HasSHA1 = true
	}
	if plen > 0 {
		p, err := decodeProps(propText)
		if err != nil {
			if spooled != "" {
				os.Remove(spooled)
			}
			return nil, 0, err
		}
		c.Props = p
	}
	if kind == "dir" {
		c.Kind = fs.KindDir
	} else {
		c.Kind = fs.KindFile
	}
	consumed := plen + tlen
	switch action {
	case "add":
		if rev, ok := hdrs["Node-copyfrom-rev"]; ok {
			rv, _ := strconv.ParseUint(rev, 10, 64)
			c.Action = fs.ActionCopy
			c.CopyFromRev = rv
			c.CopyFromPath = "/" + strings.TrimPrefix(hdrs["Node-copyfrom-path"], "/")
		} else {
			c.Action = fs.ActionAdd
		}
		return []fs.Change{c}, consumed, nil
	case "change":
		c.Action = fs.ActionModify
		return []fs.Change{c}, consumed, nil
	case "delete":
		c.Action = fs.ActionDelete
		return []fs.Change{c}, consumed, nil
	case "replace":
		// delete then add
		del := fs.Change{Path: path, Action: fs.ActionDelete, Kind: c.Kind}
		if rev, ok := hdrs["Node-copyfrom-rev"]; ok {
			rv, _ := strconv.ParseUint(rev, 10, 64)
			c.Action = fs.ActionCopy
			c.CopyFromRev = rv
			c.CopyFromPath = "/" + strings.TrimPrefix(hdrs["Node-copyfrom-path"], "/")
		} else {
			c.Action = fs.ActionAdd
		}
		return []fs.Change{del, c}, consumed, nil
	default:
		if spooled != "" {
			os.Remove(spooled)
		}
		return nil, 0, fmt.Errorf("dump: unknown node-action %q", action)
	}
}

// skipNodeContent consumes a node record's property and text payload without
// buffering it. Uses the same layout as buildChange (props then text).
func skipNodeContent(br *bufio.Reader, hdrs map[string]string) error {
	plen, _ := strconv.Atoi(hdrs["Prop-content-length"])
	tlen := 0
	if v, ok := hdrs["Text-content-length"]; ok {
		tlen, _ = strconv.Atoi(v)
	}
	n := plen + tlen
	if n <= 0 {
		return nil
	}
	if _, err := io.CopyN(io.Discard, br, int64(n)); err != nil {
		return fmt.Errorf("dump: skip node content: %w", err)
	}
	return nil
}
