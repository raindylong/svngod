package dump

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// run builds svnadmin once for the whole test binary.
var svnadminBin = func() string {
	dir, _ := os.MkdirTemp("", "svnadmin")
	bin := filepath.Join(dir, "svnadmin")
	out, err := exec.Command("go", "build", "-o", bin, "svngod/cmd/svnadmin").CombinedOutput()
	if err != nil {
		panic("build svnadmin: " + string(out))
	}
	return bin
}()

// TestSvnadminE2E exercises the full migration chain:
// dump(src memory) -> file1 -> create+load repo1 -> dump repo1 -> file2 ->
// load repo2 -> assertSameTree(src, repo2). Also asserts dump idempotence
// (file1 == file2 byte-for-byte).
func TestSvnadminE2E(t *testing.T) {
	src := buildSourceRepo(t)
	dir := t.TempDir()
	file1 := filepath.Join(dir, "a.dump")
	file2 := filepath.Join(dir, "b.dump")
	repo1 := filepath.Join(dir, "repo1")
	repo2 := filepath.Join(dir, "repo2")

	// 1. dump source to file1
	f, err := os.Create(file1)
	if err != nil {
		t.Fatal(err)
	}
	if err := Dump(src, f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	// 2. create + load repo1 from file1
	if out, err := exec.Command(svnadminBin, "create", repo1).CombinedOutput(); err != nil {
		t.Fatalf("svnadmin create: %v\n%s", err, out)
	}
	if out, err := exec.Command(svnadminBin, "load", "--input", file1, repo1).CombinedOutput(); err != nil {
		t.Fatalf("svnadmin load: %v\n%s", err, out)
	}

	// 3. dump repo1 to file2
	if out, err := exec.Command(svnadminBin, "dump", "--output", file2, repo1).CombinedOutput(); err != nil {
		t.Fatalf("svnadmin dump: %v\n%s", err, out)
	}

	// 4. load repo2 from file2
	if out, err := exec.Command(svnadminBin, "load", "--input", file2, repo2).CombinedOutput(); err != nil {
		t.Fatalf("svnadmin load repo2: %v\n%s", err, out)
	}

	// 5. verify tree equality and dump idempotence
	st, err := store.NewFileStore(filepath.Dir(repo2))
	if err != nil {
		t.Fatal(err)
	}
	dst, err := fs.OpenRepo(st, "repos/"+filepath.Base(repo2))
	if err != nil {
		t.Fatal(err)
	}
	assertSameTree(t, src, dst)

	d1, err := os.ReadFile(file1)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := os.ReadFile(file2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d1, d2) {
		t.Fatalf("dump not idempotent: len1=%d len2=%d", len(d1), len(d2))
	}
}
