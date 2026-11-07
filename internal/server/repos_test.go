package server

import (
	"testing"

	"svngod/internal/fs"
	"svngod/internal/fs/store"
)

func TestSetReposSwapsAndInvalidates(t *testing.T) {
	st := store.NewMemoryStore()
	repo, err := fs.CreateRepo(st, "repos/a", 4096, 64)
	if err != nil {
		t.Fatal(err)
	}
	opener := func(string) (*fs.Repo, error) { return fs.OpenRepo(st, "repos/a") }
	rc1 := &RepoConfig{Name: "a", OpenRepo: opener}
	s := New(Config{BasePath: "/svn", Repos: []*RepoConfig{rc1}})

	s.repoMapMu.Lock()
	s.repoCache["a"] = repo
	s.repoMapMu.Unlock()

	rc2 := &RepoConfig{Name: "a", OpenRepo: opener}
	rc3 := &RepoConfig{Name: "b", OpenRepo: opener}
	retired, err := s.SetRepos([]*RepoConfig{rc2, rc3}, map[string]bool{"a": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(retired) != 1 || retired[0] != repo {
		t.Fatalf("retired=%v", retired)
	}
	if names := s.RepoNames(); len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("names=%v", names)
	}
	if got := s.repoCfg("a"); got != rc2 {
		t.Fatalf("snapshot not swapped")
	}
	if _, err := s.SetRepos([]*RepoConfig{rc2, rc2}, nil); err == nil {
		t.Fatal("duplicate repository accepted")
	}
}
