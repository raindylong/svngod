package fs

import (
	"bytes"
	"errors"
	"sort"

	"svngod/internal/wire"
)

const revBlockMagic = "SVNGREV1"

type nodeTableEntry struct {
	ID  uint64
	Off uint64
	Len uint64
}

// RevBlock is the immutable unit holding all node-revs of one revision.
// It is the format of both a loose object and one block inside a packed shard.
type RevBlock struct {
	Rev      uint64
	PrevRev  uint64
	Root     NodeID // root directory node-rev valid at this revision
	nodeTbl  []nodeTableEntry
	payload  []byte
}

// NewRevBlock builds a rev block from nodes (any order).
func NewRevBlock(rev, prevRev uint64, root NodeID, nodes []*NodeRev) *RevBlock {
	sorted := append([]*NodeRev(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Node.ID < sorted[j].Node.ID })
	rb := &RevBlock{Rev: rev, PrevRev: prevRev, Root: root}
	var payload bytes.Buffer
	for _, nr := range sorted {
		rb.nodeTbl = append(rb.nodeTbl, nodeTableEntry{ID: nr.Node.ID, Off: uint64(payload.Len())})
		payload.Write(nr.Marshal())
		rb.nodeTbl[len(rb.nodeTbl)-1].Len = uint64(payload.Len()) - rb.nodeTbl[len(rb.nodeTbl)-1].Off
	}
	rb.payload = payload.Bytes()
	return rb
}

// Marshal serializes the rev block.
func (rb *RevBlock) Marshal() []byte {
	var nt bytes.Buffer
	for _, e := range rb.nodeTbl {
		wire.WriteUvarint(&nt, e.ID)
		wire.WriteUvarint(&nt, e.Off)
		wire.WriteUvarint(&nt, e.Len)
	}
	var b bytes.Buffer
	b.WriteString(revBlockMagic)
	wire.WriteUvarint(&b, rb.Rev)
	wire.WriteUvarint(&b, rb.PrevRev)
	wire.WriteUvarint(&b, rb.Root.Rev)
	wire.WriteUvarint(&b, rb.Root.ID)
	wire.WriteUvarint(&b, uint64(len(rb.nodeTbl)))
	wire.WriteUvarint(&b, uint64(nt.Len()))
	b.Write(nt.Bytes())
	b.Write(rb.payload)
	return b.Bytes()
}

// UnmarshalRevBlock decodes a rev block, keeping node access lazy.
func UnmarshalRevBlock(b []byte) (*RevBlock, error) {
	if len(b) < len(revBlockMagic) || string(b[:len(revBlockMagic)]) != revBlockMagic {
		return nil, errors.New("fs: bad rev-block magic")
	}
	rb := &RevBlock{}
	p := len(revBlockMagic)
	var n int
	var err error
	if rb.Rev, n, err = wire.ReadUvarint(b[p:]); err != nil {
		return nil, err
	}
	p += n
	if rb.PrevRev, n, err = wire.ReadUvarint(b[p:]); err != nil {
		return nil, err
	}
	p += n
	if rb.Root.Rev, n, err = wire.ReadUvarint(b[p:]); err != nil {
		return nil, err
	}
	p += n
	if rb.Root.ID, n, err = wire.ReadUvarint(b[p:]); err != nil {
		return nil, err
	}
	p += n
	nNodes, n, err := wire.ReadUvarint(b[p:])
	if err != nil {
		return nil, err
	}
	p += n
	ntLen, n, err := wire.ReadUvarint(b[p:])
	if err != nil {
		return nil, err
	}
	p += n
	if uint64(len(b)-p) < ntLen {
		return nil, errors.New("fs: rev-block node table truncated")
	}
	nt := b[p : p+int(ntLen)]
	p += int(ntLen)
	np := 0
	for i := uint64(0); i < nNodes; i++ {
		var e nodeTableEntry
		if e.ID, n, err = wire.ReadUvarint(nt[np:]); err != nil {
			return nil, err
		}
		np += n
		if e.Off, n, err = wire.ReadUvarint(nt[np:]); err != nil {
			return nil, err
		}
		np += n
		if e.Len, n, err = wire.ReadUvarint(nt[np:]); err != nil {
			return nil, err
		}
		np += n
		rb.nodeTbl = append(rb.nodeTbl, e)
	}
	rb.payload = b[p:]
	return rb, nil
}

// NodeIDs returns the node-rev ids in this block.
func (rb *RevBlock) NodeIDs() []uint64 {
	ids := make([]uint64, 0, len(rb.nodeTbl))
	for _, e := range rb.nodeTbl {
		ids = append(ids, e.ID)
	}
	return ids
}

// GetNode lazily parses the node-rev with the given id.
func (rb *RevBlock) GetNode(id uint64) (*NodeRev, error) {
	i := sort.Search(len(rb.nodeTbl), func(i int) bool { return rb.nodeTbl[i].ID >= id })
	if i >= len(rb.nodeTbl) || rb.nodeTbl[i].ID != id {
		return nil, errors.New("fs: node not found in rev block")
	}
	e := rb.nodeTbl[i]
	if e.Off+e.Len > uint64(len(rb.payload)) {
		return nil, errors.New("fs: node payload out of bounds")
	}
	return UnmarshalNodeRev(rb.payload[e.Off : e.Off+e.Len])
}
