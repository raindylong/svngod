package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

func TestUpdateRevPropsLooseAndPacked(t *testing.T) {
	st := store.NewMemoryStore()
	r, err := CreateRepo(st, "revp", 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	for i, msg := range []string{"one", "two"} {
		if _, err := r.CommitTxn(0, []Change{{
			Path: "/f" + string(rune('a'+i)), Action: ActionAdd, Kind: KindFile, Data: []byte("x"),
		}}, map[string]string{"svn:log": msg, "svn:author": "u", "svn:date": "2026-01-01T00:00:00Z"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "edited"}, nil); err != nil {
		t.Fatal(err)
	}
	rp, _ := r.GetRevProps(1)
	if rp.Props["svn:log"] != "edited" {
		t.Fatalf("log=%q", rp.Props["svn:log"])
	}
	if err := r.UpdateRevProps(1, nil, []string{"svn:log"}); err != nil {
		t.Fatal(err)
	}
	rp, _ = r.GetRevProps(1)
	if _, ok := rp.Props["svn:log"]; ok {
		t.Fatal("log not removed")
	}

	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "packed"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.PackShard(0); err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "packed-edited"}, nil); err != nil {
		t.Fatal(err)
	}
	rp, err = r.GetRevProps(1)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["svn:log"] != "packed-edited" {
		t.Fatalf("packed log=%q", rp.Props["svn:log"])
	}
	// Another revision in the same pack still reads correctly.
	rp0, err := r.GetRevProps(0)
	if err != nil || rp0 == nil {
		t.Fatalf("r0: %v", err)
	}
}
