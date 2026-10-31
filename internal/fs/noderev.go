// Package fs implements the repository versioning layer on top of the
// object Store: node-rev model, rev_block / shard-idx / revprops / current
// codecs, and the Repo (commit + read) API.
package fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"svngod/internal/wire"
)

// NodeKind.
const (
	KindFile byte = 1
	KindDir  byte = 2
)

// TextRep.
const (
	TextRepFull  byte = 0
	TextRepDelta byte = 1
	// TextRepExternal stores the full text (and its md5) in a separate object,
	// keeping rev blocks small and letting GET stream without materializing it.
	TextRepExternal byte = 2
)

// PropRep.
const (
	PropRepFull  byte = 0
	PropRepDelta byte = 1
)

// NodeID identifies a node-rev: it is created in a revision and, once written,
// is immutable.
type NodeID struct {
	Rev uint64
	ID  uint64
}

func (id NodeID) IsZero() bool { return id.Rev == 0 && id.ID == 0 }

// IDKey returns a stable string identifying this node-rev (for ETags).
func (id NodeID) IDKey() string { return fmt.Sprintf("%d.%d", id.Rev, id.ID) }

// Child is one directory entry.
type Child struct {
	Name string
	ID   NodeID
}

// NodeRev is an immutable node-revision.
type NodeRev struct {
	Kind         byte
	Node         NodeID
	Pred         NodeID // zero = none
	DeltaDepth   uint64
	CopyFrom     *NodeID
	CopyFromPath string
	Props        map[string]string
	TextRep      byte
	TextMD5      [16]byte
	Text         []byte
	// TextRef/TextSize describe an external text object (TextRepExternal):
	// the object key suffix and its length.
	TextRef  string
	TextSize uint64
	// TotalSize is the file's full text length, independent of the stored
	// representation (inline full text, delta bytes or external object). It
	// is written as an optional trailing field so old blocks still decode.
	TotalSize uint64
	Base      *NodeID // delta base, when TextRep==TextRepDelta
	Children  []Child // dir only
	// ByName is a runtime-only name->Child index for directories, built on
	// decode/cache; it is NOT persisted (Marshal ignores it) and lets Resolve
	// find a child in O(1) instead of scanning Children (O(n) on a big dir).
	ByName map[string]Child
}

// Marshal serializes the node-rev (with a trailing CRC32 of the body).
func (nr *NodeRev) Marshal() []byte {
	var b bytes.Buffer
	b.WriteByte(nr.Kind)
	wire.WriteUvarint(&b, nr.Node.Rev)
	wire.WriteUvarint(&b, nr.Node.ID)
	wire.WriteUvarint(&b, nr.Pred.Rev)
	wire.WriteUvarint(&b, nr.Pred.ID)
	wire.WriteUvarint(&b, nr.DeltaDepth)
	if nr.CopyFrom != nil {
		b.WriteByte(1)
		wire.WriteUvarint(&b, nr.CopyFrom.Rev)
		wire.WriteUvarint(&b, nr.CopyFrom.ID)
		wire.WriteString(&b, nr.CopyFromPath)
	} else {
		b.WriteByte(0)
	}
	// props (deterministic: sorted keys)
	names := make([]string, 0, len(nr.Props))
	for k := range nr.Props {
		names = append(names, k)
	}
	sort.Strings(names)
	wire.WriteUvarint(&b, uint64(len(names)))
	for _, k := range names {
		wire.WriteString(&b, k)
		wire.WriteString(&b, nr.Props[k])
	}
	// text
	b.WriteByte(nr.TextRep)
	b.Write(nr.TextMD5[:])
	if nr.TextRep == TextRepExternal {
		wire.WriteString(&b, nr.TextRef)
		wire.WriteUvarint(&b, nr.TextSize)
	} else {
		wire.WriteUvarint(&b, uint64(len(nr.Text)))
		b.Write(nr.Text)
	}
	if nr.TextRep == TextRepDelta && nr.Base != nil {
		wire.WriteUvarint(&b, nr.Base.Rev)
		wire.WriteUvarint(&b, nr.Base.ID)
	}
	// children
	if nr.Kind == KindDir {
		wire.WriteUvarint(&b, uint64(len(nr.Children)))
		for _, c := range nr.Children {
			wire.WriteString(&b, c.Name)
			wire.WriteUvarint(&b, c.ID.Rev)
			wire.WriteUvarint(&b, c.ID.ID)
		}
	}
	// Optional trailing field: full text size for files. Older node-revs end
	// here, so absence simply means "unknown".
	if nr.TotalSize > 0 {
		wire.WriteUvarint(&b, nr.TotalSize)
	}
	body := b.Bytes()
	out := make([]byte, 0, len(body)+4)
	out = append(out, body...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	out = append(out, crc[:]...)
	return out
}

// UnmarshalNodeRev decodes a node-rev, verifying its CRC32.
func UnmarshalNodeRev(b []byte) (*NodeRev, error) {
	if len(b) < 4 {
		return nil, errors.New("fs: node-rev too short")
	}
	body := b[:len(b)-4]
	want := binary.BigEndian.Uint32(b[len(b)-4:])
	if got := crc32.ChecksumIEEE(body); got != want {
		return nil, errors.New("fs: node-rev CRC mismatch")
	}
	nr := &NodeRev{}
	p := 0
	var n int
	var err error

	if nr.Kind, n, err = wire.ReadByte(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if nr.Node.Rev, n, err = wire.ReadUvarint(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if nr.Node.ID, n, err = wire.ReadUvarint(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if nr.Pred.Rev, n, err = wire.ReadUvarint(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if nr.Pred.ID, n, err = wire.ReadUvarint(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if nr.DeltaDepth, n, err = wire.ReadUvarint(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if hasCopy, n, err := wire.ReadByte(body[p:]); err != nil {
		return nil, err
	} else if hasCopy == 1 {
		p += n
		cf := &NodeID{}
		if cf.Rev, n, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
		p += n
		if cf.ID, n, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
		p += n
		if nr.CopyFromPath, n, err = wire.ReadString(body[p:]); err != nil {
			return nil, err
		}
		p += n
		nr.CopyFrom = cf
	} else {
		p += n
	}

	nProps, n, err := wire.ReadUvarint(body[p:])
	if err != nil {
		return nil, err
	}
	p += n
	if nr.Props == nil {
		nr.Props = map[string]string{}
	}
	for i := uint64(0); i < nProps; i++ {
		var k, v string
		if k, n, err = wire.ReadString(body[p:]); err != nil {
			return nil, err
		}
		p += n
		if v, n, err = wire.ReadString(body[p:]); err != nil {
			return nil, err
		}
		p += n
		nr.Props[k] = v
	}

	if nr.TextRep, n, err = wire.ReadByte(body[p:]); err != nil {
		return nil, err
	}
	p += n
	if len(body)-p < 16 {
		return nil, errors.New("fs: node-rev missing md5")
	}
	copy(nr.TextMD5[:], body[p:p+16])
	p += 16
	if nr.TextRep == TextRepExternal {
		if nr.TextRef, n, err = wire.ReadString(body[p:]); err != nil {
			return nil, err
		}
		p += n
		if nr.TextSize, n, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
		p += n
	} else {
		tlen, n, err := wire.ReadUvarint(body[p:])
		if err != nil {
			return nil, err
		}
		p += n
		if uint64(len(body)-p) < tlen {
			return nil, errors.New("fs: node-rev text truncated")
		}
		nr.Text = append([]byte(nil), body[p:p+int(tlen)]...)
		p += int(tlen)
	}
	if nr.TextRep == TextRepDelta {
		base := &NodeID{}
		if base.Rev, n, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
		p += n
		if base.ID, n, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
		p += n
		nr.Base = base
	}
	if nr.Kind == KindDir {
		nChild, n, err := wire.ReadUvarint(body[p:])
		if err != nil {
			return nil, err
		}
		p += n
		for i := uint64(0); i < nChild; i++ {
			var c Child
			if c.Name, n, err = wire.ReadString(body[p:]); err != nil {
				return nil, err
			}
			p += n
			if c.ID.Rev, n, err = wire.ReadUvarint(body[p:]); err != nil {
				return nil, err
			}
			p += n
			if c.ID.ID, n, err = wire.ReadUvarint(body[p:]); err != nil {
				return nil, err
			}
			p += n
			nr.Children = append(nr.Children, c)
		}
	}
	if p < len(body) {
		if nr.TotalSize, _, err = wire.ReadUvarint(body[p:]); err != nil {
			return nil, err
		}
	}
	return nr, nil
}
