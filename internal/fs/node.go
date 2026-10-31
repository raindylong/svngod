package fs

import (
	"strings"
)

// ChangeAction describes one path change in a commit.
type ChangeAction int

const (
	ActionAdd ChangeAction = iota
	ActionModify
	ActionDelete
	ActionCopy
)

// Change is one requested path change in CommitTxn.
type Change struct {
	Path         string
	Action       ChangeAction
	Kind         byte
	Data         []byte
	Props        map[string]string
	CopyFromPath string // ActionCopy
	CopyFromRev  uint64 // ActionCopy (rev of copy source)
	// BaseRev is the client's working-copy revision for this path (0 for a
	// new node); CommitTxn's per-path out-of-date check uses it.
	BaseRev uint64
	// DataFile, when set, holds the file body on disk (a streamed PUT). The
	// commit stores it as an external text without loading it into memory.
	DataFile string
	// DataMD5 is the body's md5 when the caller already computed it while
	// streaming (lets the commit skip a full hash pass).
	DataMD5 [16]byte
	HasMD5  bool
	// TextRef/TextSize point at an already-stored external text object (the
	// upload path stores it before commit so the commit only records it).
	TextRef  string
	TextSize uint64
}

// tnode is an in-memory working-copy node during a commit.
type tnode struct {
	kind         byte
	props        map[string]string
	data         []byte // file full text (as of HEAD when loaded)
	dataFile     string // streamed body on disk (large PUT)
	dataMD5      [16]byte
	hasMD5       bool
	textRef      string
	textSize     uint64
	baseText     []byte // previous file text captured during this commit (delta base)
	children     map[string]*tnode
	copyFrom     *NodeID
	copyFromPath string

	// identity / representation of the current node-rev (as of HEAD)
	id         NodeID // zero = new this commit
	pred       NodeID
	deltaDepth uint64
	textRep    byte
	base       *NodeID

	// per-commit state
	newID uint64 // assigned node id for this commit (0 = unassigned)
	dirty bool

	// lazy-load flags. A node loaded via lazyLoad keeps its directory children
	// and file text unexpanded; they are fetched on demand (ensureDir /
	// ensureData). This is what makes commits incremental: untouched subtrees
	// keep their original NodeID and are never read, so a small change to a
	// huge repository costs only the changed path's nodes (critical for
	// high-RTT backends like S3, where loading the whole HEAD tree dominates).
	dirLoaded  bool // directory children expanded
	dataLoaded bool // file full text loaded
}

func copyProps(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// loadNode reads a node's current rev and returns a lazy tnode: a directory's
// children and a file's full text are NOT fetched until ensureDir/ensureData
// are called. CommitTxn only expands the nodes on the changed path, so
// untouched subtrees keep their original NodeID and cost zero reads — this is
// what makes commits incremental and fast on high-RTT backends (S3).
func (r *Repo) loadNode(id NodeID) (*tnode, error) {
	nr, err := r.GetNodeRev(id)
	if err != nil {
		return nil, err
	}
	t := &tnode{
		kind:         nr.Kind,
		props:        copyProps(nr.Props),
		id:           nr.Node,
		pred:         nr.Pred,
		deltaDepth:   nr.DeltaDepth,
		textRep:      nr.TextRep,
		base:         nr.Base,
		copyFromPath: nr.CopyFromPath,
	}
	if nr.CopyFrom != nil {
		cf := *nr.CopyFrom
		t.copyFrom = &cf
	}
	return t, nil
}

// ensureDir expands a directory's children lazily (idempotent).
func (r *Repo) ensureDir(t *tnode) error {
	if t == nil || t.kind != KindDir || t.dirLoaded {
		return nil
	}
	if t.id.IsZero() {
		// newly created in this commit; children already initialized
		t.dirLoaded = true
		return nil
	}
	nr, err := r.GetNodeRev(t.id)
	if err != nil {
		return err
	}
	t.children = make(map[string]*tnode, len(nr.Children))
	for _, c := range nr.Children {
		ct, err := r.loadNode(c.ID)
		if err != nil {
			return err
		}
		t.children[c.Name] = ct
	}
	t.dirLoaded = true
	return nil
}

// ensureData loads a file's full text lazily (idempotent).
func (r *Repo) ensureData(t *tnode) error {
	if t == nil || t.kind != KindFile || t.dataLoaded {
		return nil
	}
	if t.dirty {
		// working-copy node already holds its final text (set by applyChange or
		// copied by deepCopy); do not overwrite it with the stored text.
		t.dataLoaded = true
		return nil
	}
	if t.id.IsZero() {
		// newly created in this commit; text already in t.data
		t.dataLoaded = true
		return nil
	}
	txt, err := r.GetText(t.id)
	if err != nil {
		return err
	}
	t.data = txt
	t.dataLoaded = true
	return nil
}

// ensureFull recursively loads a node and its entire subtree (copy sources).
func (r *Repo) ensureFull(t *tnode) error {
	if t == nil {
		return nil
	}
	if t.kind == KindFile {
		return r.ensureData(t)
	}
	if err := r.ensureDir(t); err != nil {
		return err
	}
	for _, c := range t.children {
		if err := r.ensureFull(c); err != nil {
			return err
		}
	}
	return nil
}

// segments splits a path into its components, dropping empty, "." and ".."
// segments. Dropping ".." prevents path-traversal attempts (e.g. a request for
// "/dir/../../secret") from escaping the intended repo subtree: the effective
// path never contains a parent reference.
func segments(path string) []string {
	var out []string
	for _, s := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' }) {
		if s == ".." || s == "." || s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}
