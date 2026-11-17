package svndiff

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// Regressions for the three defects fuzzing found in the svndiff decoders.
//
// Each is kept as an explicit test, not just a fuzz corpus file: a corpus entry
// only runs when someone fuzzes that target, while these run in every `go test`.
// The inputs are the minimised failing cases fuzzing produced.

// TestApplyRejectsOverflowingLength covers the slice-bounds panic: a varint of
// continuation bytes wrapped around to a value that became negative after
// int(), so a bounds check written in int passed.
func TestApplyRejectsOverflowingLength(t *testing.T) {
	src := bytes.Repeat([]byte("0"), 32)
	delta := []byte("SVN\x00\x00 0\x13\x13\x8e\x80\xff\xc1\xff\x80\xff\xc1\xc1\xc1\xc1000000000000000000000000000")
	if _, err := Apply(src, delta); err == nil {
		t.Fatal("Apply accepted a delta whose length varint overflows uint64")
	}
}

// TestApplyBoundsNewDataLength covers the second bounds bug: npos+int(length)
// wrapped negative before the slice expression.
func TestApplyBoundsNewDataLength(t *testing.T) {
	src := bytes.Repeat([]byte("0"), 32)
	delta := []byte("SVN\x00\x00 0\x13\x13\x8e\x80\x80\x80\x81\x80\x80\x80\xff\xc1\xff\x80\x95000000000000000000000000")
	if _, err := Apply(src, delta); err == nil {
		t.Fatal("Apply accepted a new-data copy with a wrapped length")
	}
}

// TestApplyTerminatesOnHugeTargetCopy covers the hang: a target copy whose
// declared length is astronomical spins the forward-copy loop long after the
// window has been proven invalid.
func TestApplyTerminatesOnHugeTargetCopy(t *testing.T) {
	src := []byte("SVN\x00\x00 0\x13\x13 \x00\x83@\xff\xc1\xc1\xc1\xc1\xc10 0000@0000000000")
	delta := []byte("SVN\x00\x00 0\x13\x13 \x00\x83@\xff\xc1\xc1\xc1\xc1\xc10 0000@000000000000000000000")
	// The assertion is that this returns at all: before the fix it looped until
	// the test binary was killed by -timeout.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = Apply(src, delta)
	}()
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("Apply did not terminate on an oversized target copy")
	}
}

// TestApplyRejectsOversizedDeclaredSection is the guard for the allocation
// bound: a 14-byte delta used to make Apply reserve a 1 TiB target view.
func TestApplyRejectsOversizedDeclaredSection(t *testing.T) {
	delta := hugeWindowDelta()
	if _, err := Apply(nil, delta); err == nil {
		t.Fatal("Apply accepted a window declaring a 1 TiB target view")
	} else if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected a size rejection, got: %v", err)
	}
}

// TestApplyToSourceTerminates is the streaming twin of the hang test. Both
// implementations had the same unbounded forward-copy loop.
func TestApplyToSourceTerminates(t *testing.T) {
	src := []byte("SVN\x00\x00 0\x13\x13 \x00\x83@\xff\xc1\xc1\xc1\xc1\xc10 0000@0000000000")
	delta := []byte("SVN\x00\x00 0\x13\x13 \x00\x83@\xff\xc1\xc1\xc1\xc1\xc10 0000@000000000000000000000")
	done := make(chan struct{})
	go func() {
		defer close(done)
		var out bytes.Buffer
		srcFn := func(off, n int64) ([]byte, error) {
			if off < 0 || n < 0 || off > int64(len(src)) || n > int64(len(src))-off {
				return nil, ioErrUnexpectedEOF
			}
			return src[off : off+n], nil
		}
		_ = ApplyToSource(int64(len(src)), srcFn, bytes.NewReader(delta), &out)
	}()
	select {
	case <-done:
	case <-timeoutAfter(t):
		t.Fatal("ApplyToSource did not terminate on an oversized target copy")
	}
}

// timeoutAfter returns a channel that closes after a bounded wait, so a hang
// fails the test instead of killing the whole binary via -timeout.
func timeoutAfter(t *testing.T) <-chan struct{} {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Second)
		close(ch)
	}()
	return ch
}

// TestReadUintRejectsOverflow pins the decoder itself, not just Apply's use of
// it. Verified the hard way: reverting only this guard left every Apply-level
// test green, because Apply's own uint64 comparison catches the same inputs
// first. The two guards overlap, so each needs its own test or one of them can
// be deleted without anything noticing.
func TestReadUintRejectsOverflow(t *testing.T) {
	// Nine continuation bytes: temp shifts left by 7 nine times, past 64 bits.
	overflow := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	if v, _, err := readUint(overflow); err == nil {
		t.Fatalf("readUint accepted an overflowing varint and returned %d", v)
	}
	// The same bytes read back as a value that would go negative in int().
	if v, _, err := readUint(overflow); err == nil && int64(v) < 0 {
		t.Fatalf("readUint returned a value that is negative as int64: %d", v)
	}
	// A legitimate large-but-valid value must still decode.
	ok := []byte{0x80 | 0x01, 0x7f} // (1<<7)|0x7f = 255
	if v, n, err := readUint(ok); err != nil || v != 255 || n != 2 {
		t.Fatalf("readUint broke on a valid input: v=%d n=%d err=%v", v, n, err)
	}
}
