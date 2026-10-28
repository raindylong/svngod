package fs

import (
	"bytes"
	"errors"
	"sort"
	"strconv"
	"strings"

	"svngod/internal/wire"
)

// RevProps holds one revision's properties (svn:author/date/log, custom).
type RevProps struct {
	Props map[string]string
}

func NewRevProps() *RevProps { return &RevProps{Props: map[string]string{}} }

// Marshal serializes revision properties (deterministic key order).
func (r *RevProps) Marshal() []byte {
	names := make([]string, 0, len(r.Props))
	for k := range r.Props {
		names = append(names, k)
	}
	sort.Strings(names)
	var b bytes.Buffer
	wire.WriteUvarint(&b, uint64(len(names)))
	for _, k := range names {
		wire.WriteString(&b, k)
		wire.WriteString(&b, r.Props[k])
	}
	return b.Bytes()
}

// UnmarshalRevProps decodes revision properties.
func UnmarshalRevProps(b []byte) (*RevProps, error) {
	r := NewRevProps()
	p := 0
	np, n, err := wire.ReadUvarint(b)
	if err != nil {
		return nil, err
	}
	p += n
	for i := uint64(0); i < np; i++ {
		var k, v string
		var nc int
		if k, nc, err = wire.ReadString(b[p:]); err != nil {
			return nil, err
		}
		p += nc
		if v, nc, err = wire.ReadString(b[p:]); err != nil {
			return nil, err
		}
		p += nc
		r.Props[k] = v
	}
	return r, nil
}

// revPropsPackEntry is one packed-revprops entry: (rev, len, payload).
type revPropsPackEntry struct {
	Rev uint64
	Off uint64
	Len uint64
}

// PackRevProps serializes a shard's revision properties into one object.
// Layout: for each revision ascending, uvarint rev, uvarint len, payload.
func PackRevProps(revProps map[uint64]*RevProps) []byte {
	revs := make([]uint64, 0, len(revProps))
	for r := range revProps {
		revs = append(revs, r)
	}
	sort.Slice(revs, func(i, j int) bool { return revs[i] < revs[j] })
	var b bytes.Buffer
	for _, r := range revs {
		payload := revProps[r].Marshal()
		wire.WriteUvarint(&b, r)
		wire.WriteUvarint(&b, uint64(len(payload)))
		b.Write(payload)
	}
	return b.Bytes()
}

// RevPropsFromPack scans a packed revprops object for the given revision.
func RevPropsFromPack(b []byte, want uint64) (*RevProps, error) {
	p := 0
	for p < len(b) {
		rev, n, err := wire.ReadUvarint(b[p:])
		if err != nil {
			return nil, err
		}
		p += n
		l, n, err := wire.ReadUvarint(b[p:])
		if err != nil {
			return nil, err
		}
		p += n
		if uint64(len(b)-p) < l {
			return nil, errors.New("fs: revprops pack truncated")
		}
		if rev == want {
			return UnmarshalRevProps(b[p : p+int(l)])
		}
		p += int(l)
	}
	return nil, errors.New("fs: revision props not found in pack")
}

// --- current (HEAD pointer) ---

// Current is the atomically-updated HEAD pointer object.
type Current struct {
	Head    uint64
	Created int64 // unix seconds
}

const currentMagic = "SVNGHEAD1"

// Marshal serializes current.
func (c *Current) Marshal() []byte {
	return []byte(currentMagic + "\nHEAD=" + strconv.FormatUint(c.Head, 10) +
		"\nCREATED=" + strconv.FormatInt(c.Created, 10) + "\n")
}

// UnmarshalCurrent decodes current.
func UnmarshalCurrent(b []byte) (*Current, error) {
	s := string(b)
	if !strings.HasPrefix(s, currentMagic+"\n") {
		return nil, errors.New("fs: bad current magic")
	}
	c := &Current{}
	for _, line := range strings.Split(s, "\n") {
		switch {
		case strings.HasPrefix(line, "HEAD="):
			v, err := strconv.ParseUint(strings.TrimPrefix(line, "HEAD="), 10, 64)
			if err != nil {
				return nil, err
			}
			c.Head = v
		case strings.HasPrefix(line, "CREATED="):
			v, err := strconv.ParseInt(strings.TrimPrefix(line, "CREATED="), 10, 64)
			if err != nil {
				return nil, err
			}
			c.Created = v
		}
	}
	return c, nil
}

// --- format (immutable, one-time) ---

// Format is the repository storage-format marker.
type Format struct {
	Version       uint64
	ShardSize     uint64
	MaxDeltaChain uint64
}

// Marshal serializes format.
func (f *Format) Marshal() []byte {
	return []byte("SVNGOBJ1\nFORMAT=" + strconv.FormatUint(f.Version, 10) +
		"\nSHARD_SIZE=" + strconv.FormatUint(f.ShardSize, 10) +
		"\nMAX_DELTA_CHAIN=" + strconv.FormatUint(f.MaxDeltaChain, 10) + "\n")
}

// UnmarshalFormat decodes format.
func UnmarshalFormat(b []byte) (*Format, error) {
	s := string(b)
	if !strings.HasPrefix(s, "SVNGOBJ1\n") {
		return nil, errors.New("fs: bad format magic")
	}
	f := &Format{}
	for _, line := range strings.Split(s, "\n") {
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return nil, err
		}
		switch key {
		case "FORMAT":
			f.Version = v
		case "SHARD_SIZE":
			f.ShardSize = v
		case "MAX_DELTA_CHAIN":
			f.MaxDeltaChain = v
		}
	}
	return f, nil
}
