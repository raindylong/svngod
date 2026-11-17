package dump

import (
	"bytes"
	"fmt"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// dumpRevisionStream renders one revision that adds path with the given text.
func dumpRevisionStream(rev uint64, path, text string) string {
	return fmt.Sprintf(`Revision-number: %d
Prop-content-length: 10
Content-length: 10

PROPS-END

Node-path: %s
Node-kind: file
Node-action: add
Prop-content-length: 10
Text-content-length: %d
Content-length: %d

PROPS-END
%s`, rev, path, len(text), 10+len(text), text)
}

// TestLoadRejectsConflictingDumpWithoutPublishing is the property AllOrNothing
// buys the importer: a dump whose revision cannot be applied must leave the
// target exactly where it was, so the operator can fix the input and retry
// instead of rebuilding from scratch.
func TestLoadRejectsConflictingDumpWithoutPublishing(t *testing.T) {
	st := store.NewMemoryStore()
	dst, err := fs.CreateRepo(st, "repos/atomic", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	header := "SVN-fs-dump-format-version: 2\n\nUUID: 11111111-2222-3333-4444-555555555555\n\n"
	// rev1 adds trunk/f; rev2 deletes a path that does not exist, which the
	// commit path rejects (delete of nonexistent path).
	body := dumpRevisionStream(1, "trunk/f", "one\n") + `Revision-number: 2
Prop-content-length: 10
Content-length: 10

PROPS-END

Node-path: trunk/missing
Node-action: delete
`
	if err := Load(dst, bytes.NewReader([]byte(header+body))); err == nil {
		t.Fatal("load accepted a dump whose second revision conflicts")
	}
	head, err := dst.Head()
	if err != nil {
		t.Fatal(err)
	}
	if head != 0 {
		t.Fatalf("head=%d after a rejected load: the whole batch must not publish", head)
	}
	// A fresh repository must still be loadable afterwards: nothing was left
	// half-written.
	if err := Load(dst, bytes.NewReader([]byte(header+dumpRevisionStream(1, "trunk/f", "one\n")))); err != nil {
		t.Fatalf("load into the same repository after a rejection: %v", err)
	}
	if h, _ := dst.Head(); h != 1 {
		t.Fatalf("head=%d after a clean retry, want 1", h)
	}
}
