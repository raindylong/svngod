package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

func TestUpdateFormatToggle(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "fmt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.DirBucketsEnabled() {
		t.Fatal("dir buckets enabled by default")
	}
	f, err := UpdateFormat(st, "fmt", func(f *Format) error {
		f.DirBuckets = true
		f.ChunkMin = 1 << 20
		f.ChunkMax = 8 << 20
		f.ChunkMask = (1 << 21) - 1
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !f.DirBuckets || f.ChunkMin != 1<<20 {
		t.Fatalf("updated format=%+v", f)
	}
	r2, err := OpenRepo(st, "fmt")
	if err != nil {
		t.Fatal(err)
	}
	if !r2.DirBucketsEnabled() {
		t.Fatal("reopened repo lost dir-buckets")
	}
	if cp := r2.ChunkParams(); cp.Min != 1<<20 || cp.Max != 8<<20 || cp.Mask != (1<<21)-1 {
		t.Fatalf("reopened chunk params=%+v", cp)
	}
}
