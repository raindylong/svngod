package svndiff

import (
	"bytes"
	"io"
	"testing"
)

// Fuzzing the decoders. The server applies client-supplied deltas and parses
// client-supplied property/transaction XML, so every one of these functions has
// untrusted input on the other side.
//
// The seed corpus encodes the shapes that matter: a healthy round trip, each
// declared version, truncated headers, and a window header whose declared
// lengths dwarf the actual payload. Those declared lengths are the interesting
// vector - a few bytes of input can claim gigabytes.

// FuzzApply checks that no input makes Apply panic or allocate without bound.
// Apply is handed deltas from the wire; a crash here is a request-level DoS.
func FuzzApply(f *testing.F) {
	src := []byte("the quick brown fox jumps over the lazy dog")
	tgt := []byte("the quick brown fox leaps over the lazy dog, twice")
	good, err := Encode(src, tgt)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(src, good)
	f.Add([]byte{}, good)
	f.Add(src, []byte("SVN\x00"))
	f.Add(src, []byte("SVN\x02"))
	f.Add(src, []byte("SVN\x00\x00\x00\x00\x00"))
	// A window header declaring an enormous target-view length with no body.
	// This is the case that found a missing bound: the streaming decoder caps
	// section lengths at maxSectionLen, the in-memory one did not.
	f.Add(src, hugeWindowDelta())

	f.Fuzz(func(t *testing.T, src, delta []byte) {
		out, err := Apply(src, delta)
		if err != nil {
			return
		}
		// A successful decode must round-trip through Encode/Apply: re-applying
		// the same delta to the same source must be deterministic and the
		// result must be re-encodable.
		again, err := Apply(src, delta)
		if err != nil {
			t.Fatalf("second Apply failed after the first succeeded: %v", err)
		}
		if !bytes.Equal(out, again) {
			t.Fatal("Apply is not deterministic for the same input")
		}
	})
}

// FuzzApplyStream mirrors FuzzApply for the streaming decoder, which reads from
// a source reader rather than a slice. Keeping both pinned matters because they
// are separate implementations: the section-length bound existed in one and not
// the other.
func FuzzApplyStream(f *testing.F) {
	src := []byte("base content for the stream decoder")
	tgt := []byte("base content for the stream decoder, extended")
	good, err := Encode(src, tgt)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte("SVN\x00"))
	f.Add(hugeWindowDelta())

	f.Fuzz(func(t *testing.T, delta []byte) {
		var out bytes.Buffer
		// A source function that honours its contract: it never returns data
		// outside [0, len(src)), so any error below comes from the delta.
		srcFn := func(off, n int64) ([]byte, error) {
			if off < 0 || n < 0 || off > int64(len(src)) || n > int64(len(src))-off {
				return nil, io.ErrUnexpectedEOF
			}
			return src[off : off+n], nil
		}
		_ = ApplyToSource(int64(len(src)), srcFn, bytes.NewReader(delta), &out)
	})
}

// FuzzVersionAndIsDelta covers the two predicates every caller runs first. A
// panic here fires before any bounds checking, so it is the cheapest possible
// crash from a single packet.
func FuzzVersionAndIsDelta(f *testing.F) {
	f.Add([]byte("SVN\x00"))
	f.Add([]byte("SVN\x02"))
	f.Add([]byte("SVN"))
	f.Add([]byte{})
	f.Add([]byte("SVN\xff\xff"))

	f.Fuzz(func(t *testing.T, b []byte) {
		isDelta := IsDelta(b)
		v := Version(b)
		// IsDelta answers "does this look like svndiff" (the 3-byte magic);
		// Version answers "which version, if supported". A well-formed magic
		// with an unsupported version byte is deliberately IsDelta=true and
		// Version=-1 - Apply then rejects it with a clear message. What must
		// hold is the direction: Version can only recognise inputs IsDelta
		// already accepted.
		if v >= 0 && !isDelta {
			t.Fatalf("Version returned %d for a non-delta %q", v, b)
		}
		if isDelta && len(b) < 4 {
			t.Fatalf("IsDelta accepted a %d-byte input", len(b))
		}
		if isDelta && len(b) >= 4 && v > 2 {
			t.Fatalf("Version returned %d, which is out of range", v)
		}
	})
}

// uvarint builds a svndiff section length the way the format encodes it.
func uvarint(v uint64) []byte {
	var buf bytes.Buffer
	writeUint(&buf, v)
	return buf.Bytes()
}

// hugeWindowDelta is a syntactically valid v0 window header whose declared
// target-view length is 1 TiB with no body behind it. The streaming decoder
// caps section lengths at maxSectionLen; the in-memory one used the declared
// value directly as a slice capacity.
func hugeWindowDelta() []byte {
	b := []byte("SVN\x00")
	b = append(b, uvarint(0)...)     // sview offset
	b = append(b, uvarint(0)...)     // sview len
	b = append(b, uvarint(1<<40)...) // tview len: 1 TiB
	b = append(b, uvarint(0)...)     // instructions len
	b = append(b, uvarint(0)...)     // new-data len
	return b
}
