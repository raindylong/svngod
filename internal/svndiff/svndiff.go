// Package svndiff implements the Subversion svndiff delta format (v0),
// matching the official encoding in subversion's libsvn_delta/svndiff.c.
//
// Stream layout (v0):
//
//	header  = 'S' 'V' 'N' 0x00
//	window* = window-header + instructions + data
//	window-header = 5 encoded uints: sview_offset, sview_len, tview_len,
//	                ins_len, data_len
//	instructions   = opcode stream (see below)
//
// Instruction: first byte's high 2 bits select the action, low 6 bits carry
// the length (0 means "extended", the length then follows as an encoded uint).
// For copy_from_source / copy_from_target the opcode is followed by an
// encoded uint offset. copy_from_new carries no offset.
package svndiff

import (
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// maybeInflate decompresses b when it carries a zlib header; a stream without
// one is returned unchanged. svn clients send the ins/data sections of an
// svndiff window zlib-compressed by default.
func maybeInflate(b []byte) ([]byte, error) {
	if len(b) >= 2 && b[0] == 0x78 && b[1]&0x20 == 0 {
		r, err := zlib.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}
	return b, nil
}

// decompressSeg decompresses an svndiff ins/data section according to the
// window version: v0 raw (tolerating an incidental zlib header), v1 zlib,
// v2 lz4 block.
func decompressSeg(version byte, b []byte) ([]byte, error) {
	switch version {
	case 1:
		r, err := zlib.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case 2:
		dst := make([]byte, 1<<20) // 1 MiB initial guess
		n, err := lz4.UncompressBlock(b, dst)
		if err == nil && n > 0 {
			return dst[:n], nil
		}
		dst = make([]byte, 1<<28) // 256 MiB fallback
		n, err = lz4.UncompressBlock(b, dst)
		if err == nil && n > 0 {
			return dst[:n], nil
		}
		// svn 1.14 sends "v2" windows with *uncompressed* ins/data sections
		// over HTTP when the window is a simple insert (see
		// send_simple_insertion_window, which never compresses). Fall back to
		// treating the section as raw bytes.
		return b, nil
	default:
		return maybeInflate(b)
	}
}

const (
	v0HeaderLen = 4
	// SVNDIFF_V0 = { 'S', 'V', 'N', 0 }; windows follow immediately (no stream header_len).
)

// windowSize is the target window size used by the encoder.
const windowSize = 102400

// minMatch is the minimum match length to emit a copy_from_source op.
const minMatch = 4

// Action codes (mirror enum svn_delta_action).
const (
	actionSource = 0 // copy from source view
	actionTarget = 1 // copy from target view
	actionNew    = 2 // insert from data
)

// op is an in-memory delta instruction.
type op struct {
	action byte
	offset uint64 // source offset, target back-distance, or new-data offset
	length uint64
}

// IsDelta reports whether data begins with a valid svndiff header.
func IsDelta(data []byte) bool {
	return len(data) >= 4 && data[0] == 'S' && data[1] == 'V' && data[2] == 'N'
}

// Version returns the svndiff version byte (0, 1 or 2), or -1 if not a delta.
func Version(data []byte) int {
	if !IsDelta(data) {
		return -1
	}
	v := int(data[3])
	if v <= 2 {
		return v
	}
	return -1
}

// --- length encoding (svn__encode_uint / svn__decode_uint) ---

// writeUint encodes val using subversion's variable-length integer encoding.
// Each following byte carries 7 bits; the lead byte's top bits encode the length
// (0x80→2 bytes, 0xc0→3, 0xe0→4, 0xf0→5), matching svn__encode_uint in svndiff.c.
func writeUint(buf *bytes.Buffer, val uint64) {
	switch {
	case val < 0x80:
		buf.WriteByte(byte(val))
	case val < 0x4000:
		buf.WriteByte(byte(0x80 | (val >> 7)))
		buf.WriteByte(byte(val & 0x7f))
	case val < 0x200000:
		buf.WriteByte(byte(0xc0 | (val >> 14)))
		buf.WriteByte(byte((val >> 7) & 0x7f))
		buf.WriteByte(byte(val & 0x7f))
	case val < 0x10000000:
		buf.WriteByte(byte(0xe0 | (val >> 21)))
		buf.WriteByte(byte((val >> 14) & 0x7f))
		buf.WriteByte(byte((val >> 7) & 0x7f))
		buf.WriteByte(byte(val & 0x7f))
	default:
		buf.WriteByte(0xf0)
		buf.WriteByte(byte((val >> 24) & 0x7f))
		buf.WriteByte(byte((val >> 16) & 0x7f))
		buf.WriteByte(byte((val >> 8) & 0x7f))
		buf.WriteByte(byte(val & 0x7f))
	}
}

// readUint decodes a variable-length integer, returning the value and bytes consumed.
func readUint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, ioErrUnexpectedEOF
	}
	c := b[0]
	var v uint64
	var n int
	switch {
	case c&0x80 == 0:
		v = uint64(c & 0x7f)
		n = 1
	case c&0xc0 == 0x80:
		if len(b) < 2 {
			return 0, 0, ioErrUnexpectedEOF
		}
		v = uint64(c&0x7f)<<7 | uint64(b[1]&0x7f)
		n = 2
	case c&0xe0 == 0xc0:
		if len(b) < 3 {
			return 0, 0, ioErrUnexpectedEOF
		}
		v = uint64(c&0x3f)<<14 | uint64(b[1]&0x7f)<<7 | uint64(b[2]&0x7f)
		n = 3
	case c&0xf0 == 0xe0:
		if len(b) < 4 {
			return 0, 0, ioErrUnexpectedEOF
		}
		v = uint64(c&0x1f)<<21 | uint64(b[1]&0x7f)<<14 | uint64(b[2]&0x7f)<<7 | uint64(b[3]&0x7f)
		n = 4
	default:
		if len(b) < 5 {
			return 0, 0, ioErrUnexpectedEOF
		}
		v = uint64(c&0x0f)<<28 | uint64(b[1]&0x7f)<<21 | uint64(b[2]&0x7f)<<14 | uint64(b[3]&0x7f)<<7 | uint64(b[4]&0x7f)
		n = 5
	}
	return v, n, nil
}

var ioErrUnexpectedEOF = errors.New("svndiff: unexpected end of input")

// --- encoder ---

type instr struct {
	action byte
	offset uint64
	length uint64
}

func encodeInstruction(buf *bytes.Buffer, it instr) {
	if it.length < 64 {
		buf.WriteByte(byte(it.action<<6 | byte(it.length)))
	} else {
		buf.WriteByte(byte(it.action << 6))
		writeUint(buf, it.length)
	}
	if it.action != actionNew {
		writeUint(buf, it.offset)
	}
}

// matchWindow computes copy_from_source / copy_from_new instructions for one
// target window against the whole source.
func matchWindow(src, tview []byte) ([]instr, []byte) {
	// Index source by 3-byte hash.
	posByHash := make(map[uint32][]int)
	if len(src) >= 3 {
		for i := 0; i+3 <= len(src); i++ {
			h := hash3(src[i:])
			posByHash[h] = append(posByHash[h], i)
		}
	}
	findMatch := func(t []byte) (int, int) {
		if len(t) < 3 {
			return 0, 0
		}
		h := hash3(t)
		bestLen, bestOff := 0, -1
		cands := posByHash[h]
		if len(cands) > 64 {
			cands = cands[:64] // bound worst-case matching cost
		}
		for _, p := range cands {
			l := 0
			for l < len(t) && p+l < len(src) && t[l] == src[p+l] {
				l++
			}
			if l > bestLen {
				bestLen, bestOff = l, p
			}
		}
		return bestLen, bestOff
	}

	var ops []instr
	var newData []byte
	newStart := -1
	flushNew := func() {
		if newStart >= 0 {
			ops = append(ops, instr{action: actionNew, length: uint64(len(newData) - newStart)})
			newStart = -1
		}
	}
	for i := 0; i < len(tview); {
		bestLen, bestOff := findMatch(tview[i:])
		if bestLen >= minMatch {
			flushNew()
			ops = append(ops, instr{action: actionSource, offset: uint64(bestOff), length: uint64(bestLen)})
			i += bestLen
		} else {
			if newStart < 0 {
				newStart = len(newData)
			}
			newData = append(newData, tview[i])
			i++
		}
	}
	flushNew()
	return ops, newData
}

func hash3(b []byte) uint32 {
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

// Encode produces a v0 svndiff delta transforming src into tgt.
func Encode(src, tgt []byte) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString("SVN")
	out.WriteByte(0)

	for base := 0; base < len(tgt); base += windowSize {
		end := base + windowSize
		if end > len(tgt) {
			end = len(tgt)
		}
		tview := tgt[base:end]
		ops, newData := matchWindow(src, tview)

		var ins bytes.Buffer
		for _, it := range ops {
			encodeInstruction(&ins, it)
		}

		// window header: sview_offset, sview_len, tview_len, ins_len, data_len
		writeUint(&out, 0)            // sview_offset (whole source)
		writeUint(&out, uint64(len(src))) // sview_len
		writeUint(&out, uint64(len(tview)))
		writeUint(&out, uint64(ins.Len()))
		writeUint(&out, uint64(len(newData)))
		out.Write(ins.Bytes())
		out.Write(newData)
	}
	return out.Bytes(), nil
}

// --- decoder ---

// Apply reconstructs tgt by applying delta to src. v0 uses raw ins/data,
// v1 uses zlib and v2 uses lz4 compression for the ins/data sections.
func Apply(src, delta []byte) ([]byte, error) {
	if !IsDelta(delta) {
		return nil, errors.New("svndiff: not a svndiff delta")
	}
	version := delta[3]
	if version > 2 {
		return nil, fmt.Errorf("svndiff: unsupported version %d", version)
	}

	p := v0HeaderLen
	var out []byte
	for p < len(delta) {
		var n int
		var err error
		var sviewOffset, sviewLen, tviewLen, insLen, dataLen uint64

		if sviewOffset, n, err = readUint(delta[p:]); err != nil {
			return nil, err
		}
		p += n
		if sviewLen, n, err = readUint(delta[p:]); err != nil {
			return nil, err
		}
		p += n
		if tviewLen, n, err = readUint(delta[p:]); err != nil {
			return nil, err
		}
		p += n
		if insLen, n, err = readUint(delta[p:]); err != nil {
			return nil, err
		}
		p += n
		if dataLen, n, err = readUint(delta[p:]); err != nil {
			return nil, err
		}
		p += n

		if sviewOffset > uint64(len(src)) || sviewLen > uint64(len(src))-sviewOffset {
			return nil, errors.New("svndiff: source window out of bounds")
		}
		if p+int(insLen)+int(dataLen) > len(delta) {
			return nil, ioErrUnexpectedEOF
		}
		sview := src[sviewOffset : sviewOffset+sviewLen]
		// Decompress the instruction and new-data sections according to the
		// svndiff version: v0 raw (tolerating an incidental zlib header),
		// v1 zlib, v2 lz4.
		ins, err := decompressSeg(version, delta[p:p+int(insLen)])
		if err != nil {
			return nil, fmt.Errorf("svndiff: bad instruction section: %w", err)
		}
		p += int(insLen)
		data, err := decompressSeg(version, delta[p:p+int(dataLen)])
		if err != nil {
			return nil, fmt.Errorf("svndiff: bad data section: %w", err)
		}
		p += int(dataLen)

		tview := make([]byte, 0, tviewLen)
		ip := 0
		npos := 0
		for ip < len(ins) {
			c := ins[ip]
			ip++
			action := (c >> 6) & 0x3
			if action == 3 {
				return nil, errors.New("svndiff: invalid action code")
			}
			length := uint64(c & 0x3f)
			if length == 0 {
				var n2 int
				if length, n2, err = readUint(ins[ip:]); err != nil {
					return nil, err
				}
				ip += n2
			}
			if length == 0 {
				return nil, errors.New("svndiff: instruction length zero")
			}
			var offset uint64
			if action != actionNew {
				var n2 int
				if offset, n2, err = readUint(ins[ip:]); err != nil {
					return nil, err
				}
				ip += n2
			}

			switch action {
			case actionSource:
				if offset > uint64(len(sview)) || length > uint64(len(sview))-offset {
					return nil, errors.New("svndiff: source copy out of bounds")
				}
				tview = append(tview, sview[offset:offset+length]...)
			case actionTarget:
				cur := len(tview)
				if offset >= uint64(cur) {
					return nil, errors.New("svndiff: target copy out of bounds")
				}
				// svn semantics: offset is an absolute position in tview
				// (svndiff.c: memmove(tview+tpos, tview+offset, length)).
				start := int(offset)
				for k := 0; k < int(length); k++ {
					tview = append(tview, tview[start+k])
				}
			case actionNew:
				if npos+int(length) > len(data) {
					return nil, errors.New("svndiff: new-data copy out of bounds")
				}
				tview = append(tview, data[npos:npos+int(length)]...)
				npos += int(length)
			}
		}
		if uint64(len(tview)) != tviewLen {
			return nil, fmt.Errorf("svndiff: window produced %d bytes, header says %d", len(tview), tviewLen)
		}
		if npos != len(data) {
			return nil, errors.New("svndiff: new-data section not fully consumed")
		}
		out = append(out, tview...)
	}
	return out, nil
}
