// Package dump implements the svnadmin dump/load interchange format (v2),
// used to migrate existing repositories into svngod's object storage backend.
//
// The wire format follows subversion's svn_fs_dump.c: a header line, then one
// block per revision, each carrying revprops and a sequence of changed paths
// with full node content. It is the standard `svnadmin dump`/`load` format.
package dump

import (
	"bytes"
	"strings"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"sort"

	"svngod/internal/fs"
)

// Diff-action kinds used internally (mirrors svn's change kinds).
type changeKind int

const (
	chAdded changeKind = iota
	chModified
	chDeleted
	chReplaced
)

// Dump writes the repository (revs 1..HEAD) to w in svn dump v2 format.
func Dump(repo *fs.Repo, w io.Writer) error {
	head, err := repo.Head()
	if err != nil {
		return err
	}
	if _, err := io.WriteString(w, "SVN-fs-dump-format-version: 2\n\n"); err != nil {
		return err
	}
	// svnadmin's loader expects the repository UUID header; without it the
	// stream is still parsed but tools such as svnrdump/svnadmin verify reject
	// it. A missing uuid is non-fatal, so dump proceeds with an empty value.
	if uuid, err := repo.UUID(); err == nil && uuid != "" {
		if _, err := fmt.Fprintf(w, "UUID: %s\n\n", uuid); err != nil {
			return err
		}
	}
	for rev := uint64(1); rev <= head; rev++ {
		if err := dumpRevision(repo, w, rev); err != nil {
			return fmt.Errorf("dump rev %d: %w", rev, err)
		}
	}
	return nil
}

func dumpRevision(repo *fs.Repo, w io.Writer, rev uint64) error {
	rp, err := repo.GetRevProps(rev)
	if err != nil {
		return err
	}
	changes, err := diffRev(repo, rev)
	if err != nil {
		return err
	}
	revPropsText := encodeProps(rp.Props)

	// A copied directory is one change in svn's history model: its subtree
	// comes with the copy. Dumping every descendant as an "add" makes
	// svnadmin load fail with E160020 (path already exists). Drop descendants
	// that are byte-identical (same node id) to their copy source; keep the
	// ones that were changed relative to it.
	type copySkip struct {
		prefix    string
		srcPrefix string
		srcRev    uint64
	}
	var skips []copySkip
	for p, act := range changes {
		if act != chAdded {
			continue
		}
		if nr, err := repo.Resolve(rev, p); err == nil && nr.CopyFrom != nil && nr.CopyFromPath != "" {
			skips = append(skips, copySkip{prefix: p, srcPrefix: nr.CopyFromPath, srcRev: nr.CopyFrom.Rev})
		}
	}
	for _, sk := range skips {
		for p := range changes {
			if !strings.HasPrefix(p, sk.prefix+"/") {
				continue
			}
			suffix := p[len(sk.prefix):]
			cur, err1 := repo.Resolve(rev, p)
			if err1 != nil {
				continue
			}
			// A WC copy may re-create every descendant as its own copy node
			// (copyfrom == the parent's source mapped to this suffix); such a
			// node is not a distinct change.
			if cur.CopyFrom != nil && cur.CopyFromPath == sk.srcPrefix+suffix && cur.CopyFrom.Rev == sk.srcRev {
				delete(changes, p)
				continue
			}
			// Otherwise skip only when the node is the very same object as the
			// copy source (shared by reference).
			src, err2 := repo.Resolve(sk.srcRev, sk.srcPrefix+suffix)
			if err2 == nil && cur.Node == src.Node {
				delete(changes, p)
			}
		}
	}

	// Node payload is written after the revision record. In the dump format
	// Content-length covers only the revision's own property block; node
	// records follow outside it. Counting node bytes here (as this code used
	// to) makes svnadmin load desynchronise and reject the stream.
	var nodes bytes.Buffer
	paths := make([]string, 0, len(changes))
	for p := range changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if _, err := writeNode(&nodes, repo, rev, p, changes[p]); err != nil {
			return err
		}
	}

	fmt.Fprintf(w, "Revision-number: %d\n", rev)
	fmt.Fprintf(w, "Prop-content-length: %d\n", len(revPropsText))
	fmt.Fprintf(w, "Content-length: %d\n\n", len(revPropsText))
	if _, err := w.Write(revPropsText); err != nil {
		return err
	}
	// Blank line separates the revision record from the first node record.
	if _, err := io.WriteString(w, "\n"); err != nil {
		return err
	}
	if _, err := w.Write(nodes.Bytes()); err != nil {
		return err
	}
	return nil
}

func writeNode(w io.Writer, repo *fs.Repo, rev uint64, path string, action changeKind) (int, error) {
	// Dump node paths are repository-relative and carry no leading slash.
	dumpPath := strings.TrimPrefix(path, "/")
	if action == chDeleted {
		// A delete carries no kind/props/content: svn_fs_dump writes just
		// Node-path + Node-action: delete followed by a blank separator line.
		fmt.Fprintf(w, "Node-path: %s\n", dumpPath)
		fmt.Fprintf(w, "Node-action: delete\n\n")
		return 0, nil
	}

	nr, err := repo.Resolve(rev, path)
	if err != nil {
		return 0, fmt.Errorf("resolve %s @%d: %w", path, rev, err)
	}
	kind := "file"
	if nr.Kind == fs.KindDir {
		kind = "dir"
	}
	actionStr := map[changeKind]string{
		chAdded: "add", chModified: "change", chReplaced: "replace",
	}[action]
	if actionStr == "" {
		return 0, fmt.Errorf("unsupported action for %s", path)
	}
	propText := encodeProps(nr.Props)
	var content []byte
	if nr.Kind == fs.KindFile {
		content, err = repo.GetText(nr.Node)
		if err != nil {
			return 0, err
		}
	}

	fmt.Fprintf(w, "Node-path: %s\n", dumpPath)
	fmt.Fprintf(w, "Node-kind: %s\n", kind)
	fmt.Fprintf(w, "Node-action: %s\n", actionStr)
	// copyfrom describes how the node came into existence; it is only valid
	// on add/replace records. A "change" node that inherited an ancestor's
	// copyfrom (the fs keeps it for lineage) must not repeat it, or loaders
	// treat the modification as a fresh copy.
	if nr.CopyFrom != nil && (action == chAdded || action == chReplaced) {
		fmt.Fprintf(w, "Node-copyfrom-rev: %d\n", nr.CopyFrom.Rev)
		fmt.Fprintf(w, "Node-copyfrom-path: %s\n", nr.CopyFromPath)
	}
	fmt.Fprintf(w, "Prop-content-length: %d\n", len(propText))
	if nr.Kind == fs.KindFile {
		fmt.Fprintf(w, "Text-content-length: %d\n", len(content))
		sum := md5.Sum(content)
		fmt.Fprintf(w, "Text-content-md5: %x\n", sum)
	}
	fmt.Fprintf(w, "Content-length: %d\n\n", len(propText)+len(content))
	if _, err := w.Write(propText); err != nil {
		return 0, err
	}
	if _, err := w.Write(content); err != nil {
		return 0, err
	}
	// blank line terminates the node block (not counted in Content-length)
	if _, err := io.WriteString(w, "\n"); err != nil {
		return 0, err
	}
	return len(propText) + len(content), nil
}

// diffRev returns the set of paths changed between rev-1 and rev.
func diffRev(repo *fs.Repo, rev uint64) (map[string]changeKind, error) {
	if rev == 0 {
		return map[string]changeKind{}, nil
	}
	out := map[string]changeKind{}
	var walk func(prevRev, rev uint64, path string) error
	childOf := func(path, name string) string { return strings.TrimSuffix(path, "/") + "/" + name }
	walk = func(prevRev, rev uint64, path string) error {
		cur, err := repo.Resolve(rev, path)
		_, prevErr := repo.Resolve(prevRev, path)
		switch {
		case err == nil && prevErr != nil:
			out[path] = chAdded
			if cur.Kind == fs.KindDir {
				children, _ := repo.ListDir(rev, path)
				for _, c := range children {
					if err := walk(prevRev, rev, childOf(path, c.Name)); err != nil {
						return err
					}
				}
			}
			return nil
		case err != nil && prevErr == nil:
			out[path] = chDeleted
			return nil
		case err != nil && prevErr != nil:
			return nil
		}
		prev, _ := repo.Resolve(prevRev, path)
		if cur.Kind != prev.Kind {
			out[path] = chReplaced
			if cur.Kind == fs.KindDir {
				children, _ := repo.ListDir(rev, path)
				for _, c := range children {
					if err := walk(prevRev, rev, childOf(path, c.Name)); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if cur.Kind == fs.KindDir {
			curCh, _ := repo.ListDir(rev, path)
			prevCh, _ := repo.ListDir(prevRev, path)
			names := map[string]bool{}
			for _, c := range curCh {
				names[c.Name] = true
			}
			for _, c := range prevCh {
				if !names[c.Name] {
					if err := walk(prevRev, rev, childOf(path, c.Name)); err != nil {
						return err
					}
				}
			}
			if !sameProps(cur.Props, prev.Props) {
				out[path] = chModified
			}
			for _, c := range curCh {
				if err := walk(prevRev, rev, childOf(path, c.Name)); err != nil {
					return err
				}
			}
		} else {
			ct, err1 := repo.GetText(cur.Node)
			pt, err2 := repo.GetText(prev.Node)
			if err1 != nil || err2 != nil {
				return fmt.Errorf("diff text: %v %v", err1, err2)
			}
			if !bytes.Equal(ct, pt) || !sameProps(cur.Props, prev.Props) {
				out[path] = chModified
			}
		}
		return nil
	}
	if err := walk(rev-1, rev, "/"); err != nil {
		return nil, err
	}
	return out, nil
}

func bytesEqual(a, b []byte) bool { return bytes.Equal(a, b) }

func sameProps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

var errNoChanges = errors.New("no changes")

// encodeProps serializes a property map in svn's dump proplist format:
//
//	K <klen>
//	<key>
//	V <vlen>
//	<value>
//	...
//	PROPS-END
func encodeProps(props map[string]string) []byte {
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		v := props[k]
		fmt.Fprintf(&b, "K %d\n%s\n", len(k), k)
		fmt.Fprintf(&b, "V %d\n%s\n", len(v), v)
	}
	b.WriteString("PROPS-END\n")
	return b.Bytes()
}
