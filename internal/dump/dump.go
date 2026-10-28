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
	payload := len(revPropsText)

	// Build node payload in a buffer so we can compute Content-length.
	var nodes bytes.Buffer
	paths := make([]string, 0, len(changes))
	for p := range changes {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		n, err := writeNode(&nodes, repo, rev, p, changes[p])
		if err != nil {
			return err
		}
		payload += n
	}

	fmt.Fprintf(w, "Revision-number: %d\n", rev)
	fmt.Fprintf(w, "Prop-content-length: %d\n", len(revPropsText))
	fmt.Fprintf(w, "Content-length: %d\n\n", payload)
	if _, err := w.Write(revPropsText); err != nil {
		return err
	}
	if _, err := w.Write(nodes.Bytes()); err != nil {
		return err
	}
	return nil
}

func writeNode(w io.Writer, repo *fs.Repo, rev uint64, path string, action changeKind) (int, error) {
	if action == chDeleted {
		// kind comes from the previous revision
		nr, err := repo.Resolve(rev-1, path)
		if err != nil {
			return 0, fmt.Errorf("delete %s: %w", path, err)
		}
		kind := "file"
		if nr.Kind == fs.KindDir {
			kind = "dir"
		}
		fmt.Fprintf(w, "Node-path: %s\n", path)
		fmt.Fprintf(w, "Node-kind: %s\n", kind)
		fmt.Fprintf(w, "Node-action: delete\n")
		fmt.Fprintf(w, "Prop-content-length: 0\n")
		fmt.Fprintf(w, "Content-length: 0\n\n")
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

	fmt.Fprintf(w, "Node-path: %s\n", path)
	fmt.Fprintf(w, "Node-kind: %s\n", kind)
	fmt.Fprintf(w, "Node-action: %s\n", actionStr)
	if nr.CopyFrom != nil {
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
