package fs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"svngod/internal/cdc"
	"svngod/internal/fs/store"
)

// failListCAS fails conditional writes to the revprop override list, modelling
// a crash between writing the override object and publishing its list entry.
type failListCAS struct {
	store.Store
	fail bool
}

func (s *failListCAS) isList(key string) bool {
	return strings.Contains(key, "/seg-revprops/") && strings.HasSuffix(key, ".idx")
}

func (s *failListCAS) CAS(ctx context.Context, key, expect string, data []byte) (string, error) {
	if s.fail && s.isList(key) {
		return "", errors.New("injected list CAS failure")
	}
	return s.Store.CAS(ctx, key, expect, data)
}

func (s *failListCAS) CreateIfAbsent(ctx context.Context, key string, data []byte) error {
	if s.fail && s.isList(key) {
		return errors.New("injected list CAS failure")
	}
	return s.Store.CreateIfAbsent(ctx, key, data)
}

// TestRevPropsUpdateFaultIsolated: an override whose list entry never landed is
// invisible to normal reads (the committed value is served), the retry heals it,
// and nothing is corrupted.
func TestRevPropsUpdateFaultIsolated(t *testing.T) {
	st := &failListCAS{Store: store.NewMemoryStore(), fail: true}
	r, err := CreateRepoSegments(st, "repos/rpf", 8, 64, cdc.DefaultParams(), false, 8)
	if err != nil {
		t.Fatal(err)
	}
	commitBatches(t, r, 1, 2)
	committed, err := r.GetRevPropsFresh(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.UpdateRevProps(1, map[string]string{"svn:log": "edited"}, nil); err == nil {
		t.Fatal("update succeeded despite the injected list failure")
	}
	// The list does not name revision 1 yet, so a normal read serves the
	// committed value: the half-written edit is invisible, not corrupt.
	r2, err := OpenRepo(st, "repos/rpf")
	if err != nil {
		t.Fatal(err)
	}
	rp, err := r2.GetRevProps(1)
	if err != nil {
		t.Fatalf("read after failed update: %v", err)
	}
	if rp.Props["svn:log"] != committed.Props["svn:log"] {
		t.Fatalf("log=%q, want committed %q", rp.Props["svn:log"], committed.Props["svn:log"])
	}
	// Healed retry publishes the edit.
	st.fail = false
	if err := r2.UpdateRevProps(1, map[string]string{"svn:log": "edited"}, nil); err != nil {
		t.Fatal(err)
	}
	rp, err = r2.GetRevPropsFresh(1)
	if err != nil {
		t.Fatal(err)
	}
	if rp.Props["svn:log"] != "edited" {
		t.Fatalf("retry log=%q", rp.Props["svn:log"])
	}
	// And the list now names revision 1.
	raw, err := st.Get(context.Background(), r2.keys.SegRevPropsIndex(0), nil)
	if err != nil {
		t.Fatal(err)
	}
	var revs []uint64
	if err := json.Unmarshal(raw, &revs); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rev := range revs {
		if rev == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("override list=%v, want revision 1", revs)
	}
	issues, err := r2.VerifyRange(1, 2, false)
	if err != nil || len(issues) != 0 {
		t.Fatalf("verify: issues=%v err=%v", issues, err)
	}
}
