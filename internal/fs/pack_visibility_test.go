package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

// TestPackedShardSeenAcrossHandles: one process packs a shard while another
// still runs. The second must notice the pack (no negative cache) and send
// revprop updates to the packed shard instead of the discarded loose object.
func TestPackedShardSeenAcrossHandles(t *testing.T) {
	st := store.NewMemoryStore()
	r1, err := CreateRepo(st, "pk", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.CommitTxn(0, []Change{{
		Path: "/a", Action: ActionAdd, Kind: KindFile, Data: []byte("a"),
	}}, map[string]string{"svn:log": "one"}); err != nil {
		t.Fatal(err)
	}
	r2, err := OpenRepo(st, "pk")
	if err != nil {
		t.Fatal(err)
	}
	if packed, _ := r2.isPacked(0); packed {
		t.Fatal("shard reported packed before packing")
	}
	if err := r1.PackShard(0); err != nil {
		t.Fatal(err)
	}
	if packed, err := r2.isPacked(0); err != nil || !packed {
		t.Fatalf("second handle did not observe the pack: packed=%v err=%v", packed, err)
	}
	if err := r2.UpdateRevProps(1, map[string]string{"pack:test": "yes"}, nil); err != nil {
		t.Fatal(err)
	}
	rp, err := r1.GetRevProps(1)
	if err != nil || rp == nil || rp.Props["pack:test"] != "yes" {
		t.Fatalf("packed revprop update not visible: %+v err=%v", rp, err)
	}
}
