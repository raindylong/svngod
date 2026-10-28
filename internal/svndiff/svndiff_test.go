package svndiff

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestEncodeApplyRoundTrip(t *testing.T) {
	src := []byte("The quick brown fox jumps over the lazy dog. " +
		"Pack my box with five dozen liquor jugs. " +
		"How vexingly quick daft zebras jump!")
	// Several edits to exercise source copies and insertions.
	tgts := [][]byte{
		append([]byte(nil), src...), // identical
		[]byte("The quick brown fox jumps over the lazy dog."),
		append([]byte("New prefix. "), src...),
		bytes.Replace(src, []byte("quick"), []byte("slow"), -1),
		bytes.Repeat(src, 3),
		[]byte("totally different content, nothing in common at all 1234567890"),
	}
	for i, tgt := range tgts {
		delta, err := Encode(src, tgt)
		if err != nil {
			t.Fatalf("case %d: Encode: %v", i, err)
		}
		if !IsDelta(delta) {
			t.Fatalf("case %d: not a delta", i)
		}
		got, err := Apply(src, delta)
		if err != nil {
			t.Fatalf("case %d: Apply: %v", i, err)
		}
		if !bytes.Equal(got, tgt) {
			t.Fatalf("case %d: round-trip mismatch: got %d bytes, want %d", i, len(got), len(tgt))
		}
	}
}

func TestApplyFromEmptySource(t *testing.T) {
	// Delta from empty source must be pure insertions.
	tgt := []byte("hello world, this is a brand new file created from nothing")
	delta, err := Encode(nil, tgt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Apply(nil, delta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, tgt) {
		t.Fatal("empty-source round-trip mismatch")
	}
}

func TestEncodeLarge(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	src := make([]byte, 500_000)
	rng.Read(src)
	// deterministic pseudo-random target with strong commonality
	tgt := make([]byte, len(src)+10_000)
	copy(tgt, src[:len(src)/2])
	copy(tgt[len(src)/2:], src[len(src)/2+5:])
	tgt[len(src)-3] = 'X'
	delta, err := Encode(src, tgt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Apply(src, delta)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, tgt) {
		t.Fatal("large round-trip mismatch")
	}
	// Sanity: delta should be much smaller than target (strong locality).
	if len(delta) > len(tgt)/2 {
		t.Logf("note: delta %d bytes vs tgt %d (not a compression test)", len(delta), len(tgt))
	}
}

func TestApplyRejectsTruncated(t *testing.T) {
	src := []byte("abcdefghijklmnopqrstuvwxyz")
	tgt := []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	delta, err := Encode(src, tgt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(src, delta[:len(delta)-1]); err == nil {
		t.Fatal("expected error for truncated delta")
	}
}

func TestApplyRejectsCorruptAction(t *testing.T) {
	// Craft a window whose first instruction has action bits 11 (invalid).
	src := []byte("source")
	bad := []byte{'S', 'V', 'N', 0, 0, 0, 0, 0, 0, 0, 0xc0} // action=3
	bad = append(bad, 0)
	if _, err := Apply(src, bad); err == nil {
		t.Fatal("expected error for invalid action code")
	}
}

func TestVersion(t *testing.T) {
	if v := Version([]byte{'S', 'V', 'N', 0, 1, 2}); v != 0 {
		t.Fatalf("v0: got %d", v)
	}
	if v := Version([]byte{'S', 'V', 'N', 1}); v != 1 {
		t.Fatalf("v1: got %d", v)
	}
	if v := Version([]byte{'X', 'Y', 'Z'}); v != -1 {
		t.Fatalf("non-delta: got %d", v)
	}
}
