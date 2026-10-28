package authz

import "testing"

func parse(t *testing.T, s string) *Config {
	t.Helper()
	cfg, err := Parse([]byte(s))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return cfg
}

func TestAuthzBasic(t *testing.T) {
	cfg := parse(t, `
[groups]
dev = alice, bob
admin = @dev, carol

[repos:/]
* = r
@admin = rw

[repos:/private]
* =
@dev = rw
`)
	// repo-wide: everyone read
	if !cfg.Authorize("dave", "repos", "/trunk", false) {
		t.Fatal("dave should read /trunk")
	}
	if cfg.Authorize("dave", "repos", "/trunk", true) {
		t.Fatal("dave must not write /trunk")
	}
	// admin (via group @dev) can write anywhere
	if !cfg.Authorize("alice", "repos", "/trunk", true) {
		t.Fatal("alice (group dev) should write /trunk")
	}
	if !cfg.Authorize("carol", "repos", "/x", true) {
		t.Fatal("carol (admin) should write")
	}
	// private: only @dev rw
	if !cfg.Authorize("alice", "repos", "/private/a", true) {
		t.Fatal("alice should write /private")
	}
	if cfg.Authorize("dave", "repos", "/private/a", false) {
		t.Fatal("dave must not read /private")
	}
}

func TestAuthzMostSpecific(t *testing.T) {
	cfg := parse(t, `
[/]
* = r

[/locked]
* =
harry = rw
`)
	if !cfg.Authorize("harry", "r", "/locked", true) {
		t.Fatal("harry should write /locked")
	}
	if cfg.Authorize("joe", "r", "/locked", false) {
		t.Fatal("joe must not read /locked (most specific section wins)")
	}
	if !cfg.Authorize("joe", "r", "/elsewhere", false) {
		t.Fatal("joe should read /elsewhere via [/]")
	}
}

// The canonical ~ example: everyone gets r via *, daniel is excluded from rw.
func TestAuthzTildeExclusion(t *testing.T) {
	cfg := parse(t, `
[/private]
* = r
~daniel = rw
`)
	if !cfg.Authorize("daniel", "r", "/private", false) {
		t.Fatal("daniel should read (matched by *)")
	}
	if cfg.Authorize("daniel", "r", "/private", true) {
		t.Fatal("daniel must not write (~daniel excludes him from rw)")
	}
	if !cfg.Authorize("sam", "r", "/private", true) {
		t.Fatal("sam should write (rw, not excluded)")
	}
}

func TestAuthzAnonymous(t *testing.T) {
	cfg := parse(t, `
[/]
$anonymous =
$authenticated = r
`)
	if cfg.Authorize("", "r", "/x", false) {
		t.Fatal("anonymous must be denied")
	}
	if !cfg.Authorize("alice", "r", "/x", false) {
		t.Fatal("authenticated should read")
	}
}

func TestAuthzGroupTransitive(t *testing.T) {
	cfg := parse(t, `
[groups]
team = @sub, alice
sub = bob

[/]
* =
@team = rw
`)
	if !cfg.Authorize("bob", "r", "/x", true) {
		t.Fatal("bob (in sub, transitively in team) should write")
	}
	if cfg.Authorize("carol", "r", "/x", false) {
		t.Fatal("carol not in any group must be denied")
	}
}

func TestAuthzContinuation(t *testing.T) {
	cfg := parse(t, `
[groups]
big = alice, bob,
      carol, dave

[/]
* =
@big = r
`)
	for _, u := range []string{"alice", "bob", "carol", "dave"} {
		if !cfg.Authorize(u, "r", "/", false) {
			t.Fatalf("%s should be in @big", u)
		}
	}
}

func TestAuthzAllReposStar(t *testing.T) {
	cfg := parse(t, `
[*:/]
* = r

[one:/]
harry = rw
`)
	if !cfg.Authorize("any", "two", "/x", false) {
		t.Fatal("all-repos [*:/] should give read")
	}
	if !cfg.Authorize("harry", "one", "/", true) {
		t.Fatal("harry should write repo one")
	}
	// write on repo two still denied (only read via *)
	if cfg.Authorize("harry", "two", "/x", true) {
		t.Fatal("harry must not write repo two")
	}
}

func TestAuthzNoMatch(t *testing.T) {
	cfg := parse(t, `[one:/]
* = r
`)
	if cfg.Authorize("x", "two", "/", false) {
		t.Fatal("repo two has no matching section → must deny")
	}
	empty := parse(t, "")
	if empty.Authorize("x", "r", "/", false) {
		t.Fatal("empty config must deny")
	}
}

// TestAuthzMaxSemantics locks in the effective-access semantics verified
// against real svnserve 1.14 (see repo investigation): access is the MAXIMUM
// over every matching rule, not last-match-wins.
func TestAuthzMaxSemantics(t *testing.T) {
	// alice = rw then * = r: alice keeps rw (verified: alice could commit)
	cfg := parse(t, "[/]\nalice = rw\n* = r\n")
	if !cfg.Authorize("alice", "r", "/", true) {
		t.Fatal("alice must write (rw wins over * = r)")
	}
	if !cfg.Authorize("bob", "r", "/", false) {
		t.Fatal("bob should read via * = r")
	}
	if !cfg.Authorize("", "r", "/", false) {
		t.Fatal("anonymous should read via * = r")
	}

	// alice = rw then alice = r: max keeps rw (verified G1: commit succeeded)
	cfg = parse(t, "[/]\nalice = rw\nalice = r\n")
	if !cfg.Authorize("alice", "r", "/", true) {
		t.Fatal("alice must write (max of rw,r is rw)")
	}

	// alice = r then * = rw: alice gets rw (verified F3: commit succeeded)
	cfg = parse(t, "[/]\nalice = r\n* = rw\n")
	if !cfg.Authorize("alice", "r", "/", true) {
		t.Fatal("alice must write (max of r,rw is rw)")
	}

	// $authenticated = r: authenticated read, anonymous denied (verified D2)
	cfg = parse(t, "[/]\n$authenticated = r\n")
	if !cfg.Authorize("alice", "r", "/", false) {
		t.Fatal("authenticated should read")
	}
	if cfg.Authorize("", "r", "/", false) {
		t.Fatal("anonymous must be denied")
	}

	// * matches authenticated users too (verified A: alice could read)
	cfg = parse(t, "[/]\n* = r\n")
	if !cfg.Authorize("alice", "r", "/", false) {
		t.Fatal("star must match authenticated alice for read")
	}
}
