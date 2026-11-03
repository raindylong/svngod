package store

import (
	"bytes"
	"context"
	"testing"
)

// TestGetRepairingHealsCorruptReplica: a content checksum failure on one
// replica is repaired from a valid copy instead of surfacing forever.
func TestGetRepairingHealsCorruptReplica(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()

	good := []byte("the-valid-payload")
	if err := a.Put(ctx, "repos/x/texts/t", []byte("corrupted")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, "repos/x/texts/t", good); err != nil {
		t.Fatal(err)
	}
	verify := func(c []byte) bool { return bytes.Equal(c, good) }
	got, err := m.GetRepairing(ctx, "repos/x/texts/t", verify)
	if err != nil || !bytes.Equal(got, good) {
		t.Fatalf("GetRepairing = %q, %v; want the valid copy", got, err)
	}
	healed, err := a.Get(ctx, "repos/x/texts/t", nil)
	if err != nil || !bytes.Equal(healed, good) {
		t.Fatalf("corrupt replica not healed: %q, %v", healed, err)
	}
	// A second call must not need to read the corrupt copy again.
	if got, err := m.GetRepairing(ctx, "repos/x/texts/t", verify); err != nil || !bytes.Equal(got, good) {
		t.Fatalf("second GetRepairing = %q, %v", got, err)
	}
}

func TestGetRepairingRejectsAllCorrupt(t *testing.T) {
	ctx := context.Background()
	a, b := NewMemoryStore(), NewMemoryStore()
	m := NewMirror([]Store{a, b})
	defer m.Close()
	a.Put(ctx, "repos/x/texts/t", []byte("bad-a"))
	b.Put(ctx, "repos/x/texts/t", []byte("bad-b"))
	_, err := m.GetRepairing(ctx, "repos/x/texts/t", func(c []byte) bool { return false })
	if err == nil {
		t.Fatal("GetRepairing accepted a store with no valid copy")
	}
}
