package dump

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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
	var props map[string]string
	inRevision := false
	var lastRev uint64

	flush := func() error {
		if !inRevision || len(changes) == 0 {
			return nil
		}
		// Incremental import: each revision commit builds on the previous one so
		// earlier revisions' tree content is preserved across shard boundaries.
		rev, err := repo.CommitTxn(lastRev, changes, props)
		if err != nil {
			return err
		}
		lastRev = rev
		changes = nil
		return nil
	}

	for {
		hdrs, err := readHeaders(br)
		if len(hdrs) == 0 {
			if err == io.EOF {
				return flush()
			}
			if err != nil {
				return err
			}
			continue // blank separator between blocks
		}
		if err != nil && err != io.EOF {
			return err
		}

		if _, ok := hdrs["Revision-number"]; ok {
			if err := flush(); err != nil {
				return err
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
		if _, ok := hdrs["UUID"]; ok && !inRevision {
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
	content := make([]byte, tlen)
	if tlen > 0 {
		if _, err := io.ReadFull(br, content); err != nil {
			return nil, 0, err
		}
	}

	c := fs.Change{Path: path, Data: content}
	if plen > 0 {
		p, err := decodeProps(propText)
		if err != nil {
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
		return nil, 0, fmt.Errorf("dump: unknown node-action %q", action)
	}
}
