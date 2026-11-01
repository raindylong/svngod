// Package cdc implements the content-defined chunking used for large file
// texts. The server and svngocli share it, so a client can split a file and
// ask the server which chunks it is missing without ever disagreeing about
// boundaries.
package cdc

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Min         = 256 << 10 // default lower bound
	Max         = 4 << 20   // default hard cap, keeps one chunk in memory
	Mask        = (1 << 20) - 1
	AbsoluteMax = 64 << 20 // hard safety cap for any configured chunk
	Magic       = "CHK1"
	HashLen     = sha256.Size
	RefLen      = 4 + HashLen
	Threshold   = 4 << 20 // texts above this are stored as chunks
)

// Params controls where a text is cut. The server advertises them and the
// client uses the same values so their chunk boundaries (and therefore dedupe)
// agree. Repositories created before this existed read back the defaults.
type Params struct {
	Min  int
	Max  int
	Mask uint64
}

// DefaultParams returns the historical 256 KiB / 1 MiB / 4 MiB profile.
func DefaultParams() Params { return Params{Min: Min, Max: Max, Mask: Mask} }

// Normalize clamps a user-supplied profile into a safe range.
func (p Params) Normalize() Params {
	if p.Min <= 0 {
		p.Min = Min
	}
	if p.Max <= 0 {
		p.Max = Max
	}
	if p.Mask == 0 {
		p.Mask = Mask
	}
	if p.Min > p.Max {
		p.Min = p.Max
	}
	if p.Max > AbsoluteMax {
		p.Max = AbsoluteMax
	}
	if p.Min > p.Max {
		p.Min = p.Max
	}
	return p
}

// Ref describes one chunk.
type Ref struct {
	Size uint32
	Hash [HashLen]byte
}

// HashHex returns the chunk's hex sha256, which is also its object key suffix.
func HashHex(r Ref) string { return fmt.Sprintf("%x", r.Hash[:]) }

// Size returns the total text length described by refs.
func Size(refs []Ref) uint64 {
	var n uint64
	for _, r := range refs {
		n += uint64(r.Size)
	}
	return n
}

// gearTable is a deterministic table for the rolling hash. It is generated
// once, so boundaries are identical across processes and restarts.
var gearTable = func() [256]uint64 {
	var t [256]uint64
	x := uint64(0x9e3779b97f4a7c15)
	for i := 0; i < 256; i++ {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		t[i] = z ^ (z >> 31)
	}
	return t
}()

// Split reads rd with the default profile.
func Split(rd io.Reader, emit func(data []byte) error) error {
	return SplitParams(DefaultParams(), rd, emit)
}

// SplitParams reads rd and calls emit for every content-defined chunk. The
// slice passed to emit is only valid for the duration of the call (the caller
// must consume or copy it); at most p.Max bytes are held at a time.
func SplitParams(p Params, rd io.Reader, emit func(data []byte) error) error {
	p = p.Normalize()
	br := bufio.NewReaderSize(rd, 1<<20)
	buf := make([]byte, 0, p.Max)
	var h uint64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := emit(buf); err != nil {
			return err
		}
		buf = buf[:0]
		h = 0
		return nil
	}
	for {
		c, err := br.ReadByte()
		if err == io.EOF {
			return flush()
		}
		if err != nil {
			return err
		}
		buf = append(buf, c)
		h = (h << 1) + gearTable[c]
		if len(buf) >= p.Max || (len(buf) >= p.Min && h&p.Mask == 0) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// Marshal encodes refs in the node-rev manifest format.
func Marshal(refs []Ref) []byte {
	b := make([]byte, 0, len(Magic)+binary.MaxVarintLen64+len(refs)*RefLen)
	b = append(b, Magic...)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(refs)))
	b = append(b, tmp[:n]...)
	for _, ref := range refs {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], ref.Size)
		b = append(b, size[:]...)
		b = append(b, ref.Hash[:]...)
	}
	return b
}

// Parse decodes a manifest produced by Marshal.
func Parse(b []byte) ([]Ref, error) {
	if len(b) < len(Magic) || string(b[:len(Magic)]) != Magic {
		return nil, errors.New("cdc: bad manifest magic")
	}
	p := len(Magic)
	count, n := binary.Uvarint(b[p:])
	if n <= 0 {
		return nil, errors.New("cdc: bad manifest count")
	}
	p += n
	if count > uint64(len(b))/RefLen+1 {
		return nil, errors.New("cdc: manifest count out of range")
	}
	refs := make([]Ref, 0, count)
	for i := uint64(0); i < count; i++ {
		if p+RefLen > len(b) {
			return nil, errors.New("cdc: manifest truncated")
		}
		ref := Ref{Size: binary.BigEndian.Uint32(b[p : p+4])}
		copy(ref.Hash[:], b[p+4:p+RefLen])
		refs = append(refs, ref)
		p += RefLen
	}
	if p != len(b) {
		return nil, errors.New("cdc: manifest trailing bytes")
	}
	return refs, nil
}
