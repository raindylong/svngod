package main

import (
	"context"
	"testing"

	"svngod/internal/fs/store"
)

func TestCountContentObjects(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	puts := map[string]int{
		"repos/demo/texts/aa":      10,
		"repos/demo/texts/bb":      20,
		"repos/demo/chunks/cc":     3,
		"repos/demo/dirs/dd":       7,
		"repos/demo/segs/xx/1.seg": 99, // not content-addressed
		"other/texts/zz":           50, // different repository
	}
	for k, n := range puts {
		if err := st.Put(ctx, k, make([]byte, n)); err != nil {
			t.Fatal(err)
		}
	}
	counts, err := countContentObjects(ctx, st, "repos/demo")
	if err != nil {
		t.Fatal(err)
	}
	if got := counts["texts"]; got.objects != 2 || got.bytes != 30 {
		t.Fatalf("texts=%+v", got)
	}
	if got := counts["chunks"]; got.objects != 1 || got.bytes != 3 {
		t.Fatalf("chunks=%+v", got)
	}
	if got := counts["dirs"]; got.objects != 1 || got.bytes != 7 {
		t.Fatalf("dirs=%+v", got)
	}
}
