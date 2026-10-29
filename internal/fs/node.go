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
}

// tnode is an in-memory working-copy node during a commit.
type tnode struct {
	kind         byte
	props        map[string]string
	data         []byte // file full text (as of HEAD when loaded)
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
	newID   uint64 // assigned node id for this commit (0 = unassigned)
	dirty   bool
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

// loadNode recursively loads the subtree rooted at id into a tnode, keeping
// each node's current node-rev identity and (for files) its full text.
func (r *Repo) loadNode(id NodeID) (*tnode, error) {
	nr, err := r.GetNodeRev(id)
	if err != nil {
		return nil, err
	}
	t := &tnode{
		kind:       nr.Kind,
		props:      copyProps(nr.Props),
		id:         nr.Node,
		pred:       nr.Pred,
		deltaDepth: nr.DeltaDepth,
		textRep:    nr.TextRep,
		base:       nr.Base,
	}
	if nr.CopyFrom != nil {
		cf := *nr.CopyFrom
		t.copyFrom = &cf
		t.copyFromPath = nr.CopyFromPath
	}
	if nr.Kind == KindFile {
		text, err := r.GetText(nr.Node)
		if err != nil {
			return nil, err
		}
		t.data = text
	} else {
		t.children = make(map[string]*tnode, len(nr.Children))
		for _, c := range nr.Children {
			child, err := r.loadNode(c.ID)
			if err != nil {
				return nil, err
			}
			t.children[c.Name] = child
		}
	}
	return t, nil
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
