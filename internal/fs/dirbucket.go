package fs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"svngod/internal/cache"
	"svngod/internal/wire"
)

// Large directories are stored as a small table plus 16 child buckets. A
// lookup fetches one bucket instead of the whole child list, and a commit
// rewrites only the buckets whose entries changed. Repositories opt in at
// creation (Format.DirBuckets); older repositories keep children inline.
const (
	dirBucketCount     = 16
	dirBucketThreshold = 1024
	dirTableMagic      = "DIRT1"
)

func dirBucketOf(name string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return int(h.Sum32() % dirBucketCount)
}

type dirTable struct {
	Total  uint64
	Refs   [dirBucketCount]string
	Counts [dirBucketCount]uint64
}

func encodeChildren(children []Child) []byte {
	sorted := append([]Child(nil), children...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b bytes.Buffer
	wire.WriteUvarint(&b, uint64(len(sorted)))
	for _, c := range sorted {
		wire.WriteString(&b, c.Name)
		wire.WriteUvarint(&b, c.ID.Rev)
		wire.WriteUvarint(&b, c.ID.ID)
	}
	return b.Bytes()
}

func decodeChildren(b []byte) ([]Child, error) {
	p := 0
	n, k, err := wire.ReadUvarint(b[p:])
	if err != nil {
		return nil, err
	}
	p += k
	out := make([]Child, 0, n)
	for i := uint64(0); i < n; i++ {
		var c Child
		if c.Name, k, err = wire.ReadString(b[p:]); err != nil {
			return nil, err
		}
		p += k
		if c.ID.Rev, k, err = wire.ReadUvarint(b[p:]); err != nil {
			return nil, err
		}
		p += k
		if c.ID.ID, k, err = wire.ReadUvarint(b[p:]); err != nil {
			return nil, err
		}
		p += k
		out = append(out, c)
	}
	if p != len(b) {
		return nil, errors.New("fs: dir bucket trailing bytes")
	}
	return out, nil
}

func encodeDirTable(t *dirTable) []byte {
	var b bytes.Buffer
	b.WriteString(dirTableMagic)
	wire.WriteUvarint(&b, t.Total)
	for i := 0; i < dirBucketCount; i++ {
		wire.WriteString(&b, t.Refs[i])
		wire.WriteUvarint(&b, t.Counts[i])
	}
	return b.Bytes()
}

func decodeDirTable(b []byte) (*dirTable, error) {
	if len(b) < len(dirTableMagic) || string(b[:len(dirTableMagic)]) != dirTableMagic {
		return nil, errors.New("fs: bad dir table magic")
	}
	p := len(dirTableMagic)
	t := &dirTable{}
	var k int
	var err error
	if t.Total, k, err = wire.ReadUvarint(b[p:]); err != nil {
		return nil, err
	}
	p += k
	for i := 0; i < dirBucketCount; i++ {
		if t.Refs[i], k, err = wire.ReadString(b[p:]); err != nil {
			return nil, err
		}
		p += k
		if t.Counts[i], k, err = wire.ReadUvarint(b[p:]); err != nil {
			return nil, err
		}
		p += k
	}
	if p != len(b) {
		return nil, errors.New("fs: dir table trailing bytes")
	}
	return t, nil
}

// storeDirObject writes a content-addressed directory object and returns its
// hex sha256.
func (r *Repo) storeDirObject(ctx context.Context, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	ref := hex.EncodeToString(sum[:])
	key := r.keys.Dir(ref)
	ok, err := r.store.Exists(ctx, key)
	if err != nil {
		return "", err
	}
	if !ok {
		if err := r.store.Put(ctx, key, data); err != nil {
			return "", err
		}
	}
	return ref, nil
}

func (r *Repo) getDirObject(ref string) ([]byte, error) {
	if ref == "" {
		return nil, errors.New("fs: empty dir object ref")
	}
	k := cache.Key("dir/" + ref)
	if b, ok := r.cache.Get(k); ok {
		return b, nil
	}
	b, err := r.store.Get(context.Background(), r.keys.Dir(ref), nil)
	if err != nil {
		return nil, err
	}
	cachePut(r.cache, k, b)
	return b, nil
}

// dirTableOf returns the paging table of a directory node, or nil for inline
// directories.
func (r *Repo) dirTableOf(nr *NodeRev) (*dirTable, error) {
	if nr == nil || nr.Kind != KindDir || nr.TextRep != TextRepExternal || nr.TextRef == "" {
		return nil, nil
	}
	b, err := r.getDirObject(nr.TextRef)
	if err != nil {
		return nil, err
	}
	return decodeDirTable(b)
}

// dirChild looks up one child in a (possibly paged) directory, reading only
// the bucket that contains the name.
func (r *Repo) dirChild(nr *NodeRev, name string) (Child, bool, error) {
	t, err := r.dirTableOf(nr)
	if err != nil {
		return Child{}, false, err
	}
	if t == nil {
		for _, c := range nr.Children {
			if c.Name == name {
				return c, true, nil
			}
		}
		return Child{}, false, nil
	}
	i := dirBucketOf(name)
	if t.Refs[i] == "" {
		return Child{}, false, nil
	}
	b, err := r.getDirObject(t.Refs[i])
	if err != nil {
		return Child{}, false, err
	}
	children, err := decodeChildren(b)
	if err != nil {
		return Child{}, false, err
	}
	for _, c := range children {
		if c.Name == name {
			return c, true, nil
		}
	}
	return Child{}, false, nil
}

// dirChildren materializes every child of a directory node.
func (r *Repo) dirChildren(nr *NodeRev) ([]Child, error) {
	t, err := r.dirTableOf(nr)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nr.Children, nil
	}
	out := make([]Child, 0, t.Total)
	for i := 0; i < dirBucketCount; i++ {
		if t.Refs[i] == "" {
			continue
		}
		b, err := r.getDirObject(t.Refs[i])
		if err != nil {
			return nil, err
		}
		children, err := decodeChildren(b)
		if err != nil {
			return nil, err
		}
		if uint64(len(children)) != t.Counts[i] {
			return nil, fmt.Errorf("fs: dir bucket %d has %d children, table says %d", i, len(children), t.Counts[i])
		}
		out = append(out, children...)
	}
	if uint64(len(out)) != t.Total {
		return nil, fmt.Errorf("fs: directory has %d children, table says %d", len(out), t.Total)
	}
	return out, nil
}

func childrenEqual(a map[string]Child, b map[string]Child) bool {
	if len(a) != len(b) {
		return false
	}
	for name, ca := range a {
		cb, ok := b[name]
		if !ok || ca.ID != cb.ID {
			return false
		}
	}
	return true
}

// writeDirBuckets stores the directory as a table plus buckets, reusing the
// prior bucket objects for groups whose entries did not change.
func (r *Repo) writeDirBuckets(ctx context.Context, t *tnode, rev uint64) (string, int, error) {
	if t.priorChildren == nil {
		t.priorChildren = map[string]*tnode{}
	}
	newGroups := make([]map[string]Child, dirBucketCount)
	for i := range newGroups {
		newGroups[i] = map[string]Child{}
	}
	for name, c := range t.children {
		id := c.id
		if c.dirty {
			id = NodeID{Rev: rev, ID: c.newID}
		}
		newGroups[dirBucketOf(name)][name] = Child{Name: name, ID: id}
	}
	priorGroups := make([]map[string]Child, dirBucketCount)
	for i := range priorGroups {
		priorGroups[i] = map[string]Child{}
	}
	for name, c := range t.priorChildren {
		priorGroups[dirBucketOf(name)][name] = Child{Name: name, ID: c.id}
	}
	var priorRefs [dirBucketCount]string
	var priorCounts [dirBucketCount]uint64
	if t.priorTableRef != "" {
		if b, err := r.getDirObject(t.priorTableRef); err == nil {
			if pt, err := decodeDirTable(b); err == nil {
				priorRefs, priorCounts = pt.Refs, pt.Counts
			}
		}
	}
	var table dirTable
	table.Total = uint64(len(t.children))
	for i := 0; i < dirBucketCount; i++ {
		if childrenEqual(newGroups[i], priorGroups[i]) && priorRefs[i] != "" {
			table.Refs[i], table.Counts[i] = priorRefs[i], priorCounts[i]
			continue
		}
		kids := make([]Child, 0, len(newGroups[i]))
		for _, c := range newGroups[i] {
			kids = append(kids, c)
		}
		if len(kids) == 0 {
			continue
		}
		data := encodeChildren(kids)
		ref, err := r.storeDirObject(ctx, data)
		if err != nil {
			return "", 0, err
		}
		table.Refs[i], table.Counts[i] = ref, uint64(len(kids))
	}
	data := encodeDirTable(&table)
	ref, err := r.storeDirObject(ctx, data)
	if err != nil {
		return "", 0, err
	}
	return ref, len(data), nil
}
