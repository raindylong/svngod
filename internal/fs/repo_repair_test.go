package fs

import (
	"bytes"
	"context"
	"testing"

	"svngod/internal/fs/store"
)

// TestStreamTextRepairsCorruptReplica: the plain file GET path (svn cat,
// checkout, export) must not stream bytes that fail the checksum either.
func TestStreamTextRepairsCorruptReplica(t *testing.T) {
	ctx := context.Background()
	a, b := store.NewMemoryStore(), store.NewMemoryStore()
	ms := store.NewMirror([]store.Store{a, b})
	defer ms.Close()
	r, err := CreateRepo(ms, "sx", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("abcdefgh"), 40000) // 320KiB external text
	rev, err := r.CommitTxn(0, []Change{{
		Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: big,
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(rev, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepExternal {
		t.Fatalf("text rep = %d, want external", nr.TextRep)
	}
	key := r.Keys().Text(nr.TextRef)
	if err := a.Put(ctx, key, make([]byte, nr.TextSize)); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(ms, "sx")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := r2.StreamText(nr.Node, &buf); err != nil {
		t.Fatalf("StreamText: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), big) {
		t.Fatal("StreamText sent different bytes than committed")
	}
	healed, err := a.Get(ctx, key, nil)
	if err != nil || !bytes.Equal(healed, big) {
		t.Fatalf("corrupt primary not healed: len=%d err=%v", len(healed), err)
	}
}
func TestGetTextRepairsCorruptReplica(t *testing.T) {
	ctx := context.Background()
	a, b := store.NewMemoryStore(), store.NewMemoryStore()
	ms := store.NewMirror([]store.Store{a, b})
	defer ms.Close()
	r, err := CreateRepo(ms, "fx", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("0123456789abcdef"), 20000) // 320KiB, external text
	rev, err := r.CommitTxn(0, []Change{{
		Path: "/big.bin", Action: ActionAdd, Kind: KindFile, Data: big,
	}}, map[string]string{"svn:log": "big"})
	if err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(rev, "/big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if nr.TextRep != TextRepExternal {
		t.Fatalf("text rep = %d, want external (%d)", nr.TextRep, TextRepExternal)
	}
	key := r.Keys().Text(nr.TextRef)
	if err := a.Put(ctx, key, make([]byte, nr.TextSize)); err != nil {
		t.Fatal(err)
	}

	// A fresh handle so the text is not served from the in-memory cache.
	r2, err := OpenRepo(ms, "fx")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r2.GetText(nr.Node)
	if err != nil {
		t.Fatalf("GetText: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatal("GetText returned different bytes than committed")
	}
	healed, err := a.Get(ctx, key, nil)
	if err != nil || !bytes.Equal(healed, big) {
		t.Fatalf("corrupt primary not healed: len=%d err=%v", len(healed), err)
	}
}
