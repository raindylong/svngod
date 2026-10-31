package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

func TestCopyFromRev(t *testing.T) {
	st := store.NewMemoryStore()
	r, _ := CreateRepo(st, "cpref", 4096, 64)
	b1, _ := r.CommitTxn(0, []Change{{Path: "/a.txt", Action: ActionAdd, Kind: KindFile, Data: []byte("v1")}}, nil)
	if _, err := r.CommitTxn(b1, []Change{{Path: "/a.txt", Action: ActionModify, Kind: KindFile, Data: []byte("v2")}}, nil); err != nil {
		t.Fatal(err)
	}
	r2, err := r.CommitTxn(2, []Change{{Path: "/b.txt", Action: ActionCopy, Kind: KindFile, CopyFromPath: "/a.txt", CopyFromRev: 2}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nr, err := r.Resolve(r2, "/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("copyfrom rev=%d node=%v copyfrom=%+v", nr.CopyFromRev, nr.Node, nr.CopyFrom)
	if nr.CopyFromRev != 2 {
		t.Fatalf("want CopyFromRev 2, got %d", nr.CopyFromRev)
	}
}
