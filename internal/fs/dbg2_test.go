package fs

import (
	"testing"

	"svngod/internal/fs/store"
)

func TestDbgBothLost(t *testing.T) {
	a, b, c := store.NewMemoryStore(), &switchStore{Store: store.NewMemoryStore()}, &switchStore{Store: store.NewMemoryStore()}
	ms := store.NewMirror([]store.Store{a, b, c}, store.WithWriteQuorum(2))
	r, err := CreateRepo(ms, "db2", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := r.Head()
	b.down.Store(true)
	c.down.Store(true)
	revs, cerr := chaosCommit(r, base, 4)
	t.Logf("commit revs=%v err=%v", revs, cerr)
	b.down.Store(false)
	c.down.Store(false)
	r2, err := OpenRepo(ms, "db2")
	if err != nil {
		t.Fatal(err)
	}
	h, _ := r2.HeadFresh()
	t.Logf("head after reopen = %d", h)
	_, err = r2.CommitTxn(h, []Change{{Path: "/next", Action: ActionAdd, Kind: KindFile, Data: []byte("x")}}, nil)
	t.Logf("next commit err = %v", err)
}
