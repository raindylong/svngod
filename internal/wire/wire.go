// Package wire provides the low-level variable-length integer and
// length-prefixed byte/string encodings used by the repository storage
// format (svngod's own format, not svndiff).
package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// ErrTruncated is returned when a buffer ends before a value completes.
var ErrTruncated = errors.New("wire: truncated buffer")

// WriteUvarint appends a Go varint to buf.
func WriteUvarint(buf *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	buf.Write(tmp[:n])
}

// ReadUvarint reads a Go varint, returning the value and bytes consumed.
func ReadUvarint(b []byte) (uint64, int, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		if n == 0 {
			return 0, 0, io.ErrUnexpectedEOF
		}
		return 0, 0, errors.New("wire: varint overflow")
	}
	return v, n, nil
}

// WriteBytes appends a length-prefixed byte slice.
func WriteBytes(buf *bytes.Buffer, b []byte) {
	WriteUvarint(buf, uint64(len(b)))
	buf.Write(b)
}

// ReadBytes reads a length-prefixed byte slice.
func ReadBytes(b []byte) ([]byte, int, error) {
	n, c, err := ReadUvarint(b)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(b)-c) < n {
		return nil, 0, ErrTruncated
	}
	return b[c : c+int(n)], c + int(n), nil
}

// WriteString appends a length-prefixed string.
func WriteString(buf *bytes.Buffer, s string) {
	WriteBytes(buf, []byte(s))
}

// ReadString reads a length-prefixed string.
func ReadString(b []byte) (string, int, error) {
	bs, n, err := ReadBytes(b)
	if err != nil {
		return "", 0, err
	}
	return string(bs), n, nil
}

// ReadUvarintAs: convenience to read and also verify a byte is consumed.
func ReadByte(b []byte) (byte, int, error) {
	if len(b) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return b[0], 1, nil
}
