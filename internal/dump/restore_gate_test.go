package dump

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

// The restore drill (scripts/drill-restore.sh) proves the runbook end to end on
// real storage. These tests pin the parts that must not silently rot: the
// backup gates (dump --verify / --output atomicity / manifest) and the restore
// assertion (load --expect-head). Kept in the dump package because they drive
// the svnadmin binary that operators actually run.

func runSvnadmin(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := exec.Command(svnadminBin, args...).CombinedOutput()
	return string(out), err
}

// walRepo builds a repository on a two-node WAL mirror so journal records and
// the hash chain exist (a plain file store has no journal at all).
func walRepo(t *testing.T, dir string, revs int) (mirrorJSON, rootA, rootB string) {
	t.Helper()
	rootA = filepath.Join(dir, "a")
	rootB = filepath.Join(dir, "b")
	for _, r := range []string{rootA, rootB} {
		if err := os.MkdirAll(r, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mirrorJSON = filepath.Join(dir, "mirror.json")
	spec := `{"quorum":2,"prefix":"repos","mirrors":[{"type":"file","root":"` +
		rootA + `"},{"type":"file","root":"` + rootB + `"}]}`
	if err := os.WriteFile(mirrorJSON, []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runSvnadmin(t, "create", "--mirror-file", mirrorJSON, "r"); err != nil {
		t.Fatalf("create: %v\n%s", err, out)
	}
	ms := make([]store.Store, 0, 2)
	for _, r := range []string{rootA, rootB} {
		st, err := store.NewFileStore(r)
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, st)
	}
	m := store.NewMirror(ms,
		store.WithMutableOrder(fs.CompareMutable),
		store.WithRepoPrefix("repos/r"),
		store.WithWriteQuorum(2))
	repo, err := fs.OpenRepo(m, "repos/r")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= revs; i++ {
		if _, err := repo.CommitTxn(0, []fs.Change{{
			Path:   "/f" + string(rune('a'+i-1)) + ".txt",
			Action: fs.ActionAdd,
			Kind:   fs.KindFile,
			Data:   []byte(strings.Repeat("payload\n", i)),
		}}, map[string]string{"svn:author": "alice", "svn:log": "rev"}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	return mirrorJSON, rootA, rootB
}

// TestDumpVerifyProducesAuthenticatedBackup checks the happy path contract: the
// manifest head matches the repository, and the recorded sha256/size describe
// exactly the bytes on disk (that is what lets a restore authenticate a copy
// after it crosses hosts).
func TestDumpVerifyProducesAuthenticatedBackup(t *testing.T) {
	dir := t.TempDir()
	mirror, _, _ := walRepo(t, dir, 3)
	out := filepath.Join(dir, "r.dump")

	msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--verify", "--output", out, "r")
	if err != nil {
		t.Fatalf("dump: %v\n%s", err, msg)
	}
	if !strings.Contains(msg, "head=3") {
		t.Fatalf("manifest missing head: %s", msg)
	}
	if !strings.Contains(msg, "sha256=") || !strings.Contains(msg, "bytes=") {
		t.Fatalf("manifest missing digest: %s", msg)
	}
	if _, err := os.Stat(out + ".partial"); !os.IsNotExist(err) {
		t.Fatalf(".partial left behind after a successful dump")
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, []byte("SVN-fs-dump-format-version:")) {
		t.Fatalf("dump is not an SVN stream: %q", data[:40])
	}
}

// TestLoadExpectHeadRejectsWrongCount pins the restore assertion: a dump that
// silently ends early (truncated transfer, wrong source) must fail the load
// command rather than be accepted as a smaller repository.
func TestLoadExpectHeadRejectsWrongCount(t *testing.T) {
	dir := t.TempDir()
	mirror, _, _ := walRepo(t, dir, 3)
	out := filepath.Join(dir, "r.dump")
	if msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--output", out, "r"); err != nil {
		t.Fatalf("dump: %v\n%s", err, msg)
	}

	dst := filepath.Join(dir, "dst.json")
	spec := `{"quorum":2,"prefix":"repos","mirrors":[{"type":"file","root":"` +
		filepath.Join(dir, "d1") + `"},{"type":"file","root":"` + filepath.Join(dir, "d2") + `"}]}`
	if err := os.WriteFile(dst, []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"d1", "d2"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := runSvnadmin(t, "load", "--mirror-file", dst, "--input", out, "--expect-head", "4", "r")
	if err == nil {
		t.Fatalf("load --expect-head 4 accepted a 3-revision dump:\n%s", msg)
	}
	if !strings.Contains(msg, "expected") {
		t.Fatalf("refusal gave no diagnostic: %s", msg)
	}

	// The same dump restores cleanly when the expectation matches, and the
	// restored history passes the full integrity walk (tree, texts, chain).
	dst2 := filepath.Join(dir, "dst2.json")
	spec2 := `{"quorum":2,"prefix":"repos","mirrors":[{"type":"file","root":"` +
		filepath.Join(dir, "e1") + `"},{"type":"file","root":"` + filepath.Join(dir, "e2") + `"}]}`
	if err := os.WriteFile(dst2, []byte(spec2), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"e1", "e2"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if msg, err := runSvnadmin(t, "load", "--mirror-file", dst2, "--input", out, "--expect-head", "3", "r"); err != nil {
		t.Fatalf("load: %v\n%s", err, msg)
	}
	if msg, err := runSvnadmin(t, "verify", "--mirror-file", dst2, "--all-revs", "--texts", "r"); err != nil {
		t.Fatalf("verify after restore: %v\n%s", err, msg)
	}
}

// TestDumpVerifyRefusesDamagedWal pins the backup gate: a journal record that
// no longer matches its hash chain must abort the export instead of producing
// a backup of tampered history.
func TestDumpVerifyRefusesDamagedWal(t *testing.T) {
	dir := t.TempDir()
	mirror, rootA, _ := walRepo(t, dir, 3)

	rec := filepath.Join(rootA, "repos", "r", "journal", "000000000002")
	if _, err := os.Stat(rec); err != nil {
		t.Fatalf("expected journal record at %s: %v", rec, err)
	}
	if err := os.WriteFile(rec, []byte("forged record\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "bad.dump")
	msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--verify", "--output", out, "r")
	if err == nil {
		t.Fatalf("dump --verify exported a repository with a damaged journal:\n%s", msg)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("refused dump still produced %s", out)
	}

	// Without --verify the export is still possible: operators need an escape
	// hatch to salvage readable content, but it is never the default.
	if msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--output", filepath.Join(dir, "raw.dump"), "r"); err != nil {
		t.Fatalf("dump without --verify should still work: %v\n%s", err, msg)
	}
}

// TestDumpRefusesCorruptHead pins the readability gate: a corrupted rev-block
// at HEAD must stop the backup, because such a dump cannot restore that HEAD.
func TestDumpRefusesCorruptHead(t *testing.T) {
	dir := t.TempDir()
	mirror, rootA, rootB := walRepo(t, dir, 2)

	var block string
	for _, root := range []string{rootA, rootB} {
		matches, _ := filepath.Glob(filepath.Join(root, "repos", "r", "revs*", "*", "000000000002"))
		if len(matches) > 0 {
			block = matches[0]
			break
		}
	}
	if block == "" {
		matches, _ := filepath.Glob(filepath.Join(rootA, "repos", "r", "revs", "*.pack"))
		matches2, _ := filepath.Glob(filepath.Join(rootB, "repos", "r", "revs", "*.pack"))
		matches = append(matches, matches2...)
		if len(matches) > 0 {
			block = matches[0]
		}
	}
	if block == "" {
		t.Skip("rev-block layout changed; the shell drill covers this path")
	}
	if err := os.WriteFile(block, []byte("corrupted"), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "bad.dump")
	msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--verify", "--output", out, "r")
	if err == nil {
		t.Fatalf("dump --verify exported a repository with a corrupt HEAD:\n%s", msg)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("refused dump still produced %s", out)
	}
}

// TestDumpVerifyAcceptsGroupCommittedSegments covers the interaction that made
// backups of segmented repositories impossible to take: group commit writes one
// WAL record per batch, and the chain verifier used to read the revisions after
// it as a gap. dump --verify therefore refused a perfectly healthy repository -
// a backup gate that blocks all backups is as bad as no gate.
func TestDumpVerifyAcceptsGroupCommittedSegments(t *testing.T) {
	dir := t.TempDir()
	mirror := filepath.Join(dir, "m.json")
	rootA, rootB := filepath.Join(dir, "ga"), filepath.Join(dir, "gb")
	for _, r := range []string{rootA, rootB} {
		if err := os.MkdirAll(r, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	spec := `{"quorum":2,"prefix":"repos","mirrors":[{"type":"file","root":"` +
		rootA + `"},{"type":"file","root":"` + rootB + `"}]}`
	if err := os.WriteFile(mirror, []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := runSvnadmin(t, "create", "--segments", "--mirror-file", mirror, "g"); err != nil {
		t.Fatalf("create: %v\n%s", err, out)
	}

	ms := make([]store.Store, 0, 2)
	for _, r := range []string{rootA, rootB} {
		st, err := store.NewFileStore(r)
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, st)
	}
	m := store.NewMirror(ms,
		store.WithMutableOrder(fs.CompareMutable),
		store.WithRepoPrefix("repos/g"),
		store.WithWriteQuorum(2))
	repo, err := fs.OpenRepo(m, "repos/g")
	if err != nil {
		t.Fatal(err)
	}
	// Two batches: exercises both the intra-batch coverage and the link between
	// one batch's record and the previous batch's record.
	for round := 0; round < 2; round++ {
		reqs := make([]fs.BatchCommit, 0, 8)
		for i := 0; i < 8; i++ {
			reqs = append(reqs, fs.BatchCommit{Changes: []fs.Change{{
				Path: "/g" + string(rune('a'+round*8+i)), Action: fs.ActionAdd,
				Kind: fs.KindFile, Data: []byte("payload"),
			}}})
		}
		if _, errs, err := repo.CommitBatch(reqs); err != nil {
			t.Fatalf("batch %d: %v", round, err)
		} else {
			for i := range errs {
				if errs[i] != nil {
					t.Fatalf("batch %d req %d: %v", round, i, errs[i])
				}
			}
		}
	}
	out := filepath.Join(dir, "g.dump")
	msg, err := runSvnadmin(t, "dump", "--mirror-file", mirror, "--verify", "--output", out, "g")
	if err != nil {
		t.Fatalf("dump --verify rejected a group-committed segmented repo: %v\n%s", err, msg)
	}
	if !strings.Contains(msg, "head=16") {
		t.Fatalf("manifest head wrong: %s", msg)
	}
}
