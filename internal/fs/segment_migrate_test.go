package fs

import (
	"context"
	"fmt"
	"testing"

	"svngod/internal/fs/store"
)

func TestMigrateToSegments(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "mig", 8, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "mig")
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, err := r.CommitBatch(batchAdds("/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j")); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	if r.segments {
		t.Fatal("v1 repo already segmented")
	}
	n, err := r.MigrateToSegments(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 || !r.segments {
		t.Fatalf("migrated=%d segments=%v", n, r.segments)
	}
	for rev := uint64(1); rev <= 10; rev++ {
		if _, err := r.readBlock(rev); err != nil {
			t.Fatalf("read %d: %v", rev, err)
		}
		if _, err := r.loadRevProps(rev); err != nil {
			t.Fatalf("props %d: %v", rev, err)
		}
	}
	if issues, err := r.VerifyRange(1, 10, false); err != nil || len(issues) != 0 {
		t.Fatalf("verify issues=%v err=%v", issues, err)
	}
	// A fresh open uses the v2 path; a new commit continues in segments.
	r2, err := OpenRepo(st, "mig")
	if err != nil {
		t.Fatal(err)
	}
	if !r2.segments {
		t.Fatal("format not switched")
	}
	if rev, errs, err := r2.CommitBatch(batchAdds("/k")); err != nil || errs[0] != nil || rev[0] != 11 {
		t.Fatalf("post-migration commit rev=%v errs=%v err=%v", rev, errs, err)
	}
	if _, err := r2.readBlock(10); err != nil {
		t.Fatal(err)
	}
}

func TestMigratePurgeRemovesV1Objects(t *testing.T) {
	st := store.NewMemoryStore()
	if _, err := CreateRepo(st, "migp", 8, 64); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRepo(st, "migp")
	if err != nil {
		t.Fatal(err)
	}
	if _, errs, err := r.CommitBatch(batchAdds(fmt.Sprintf("/f%d", 1), "/f2", "/f3")); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	keyRev1 := r.keys.LooseRev(0, 1)
	if _, ok, _ := st.Head(context.Background(), keyRev1); !ok {
		t.Fatal("v1 object missing before migration")
	}
	if _, err := r.MigrateToSegments(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Head(context.Background(), keyRev1); ok {
		t.Fatal("v1 loose object survived purge")
	}
	if _, ok, _ := st.Head(context.Background(), r.keys.LooseRev(0, 0)); !ok {
		t.Fatal("revision 0 must stay loose")
	}
}
