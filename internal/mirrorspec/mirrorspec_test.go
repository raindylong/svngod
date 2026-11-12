package mirrorspec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestParseAcceptsBothCredentialSpellings pins the compatibility rule that
// caused a real outage-in-waiting: a storage.mirrors block copied out of the
// server config uses access-key/secret-key, while --mirror-file's own docs used
// access/secret. Both must resolve to the same credentials.
func TestParseAcceptsBothCredentialSpellings(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"config style": `{"quorum":2,"prefix":"svn-ext","mirrors":[
			{"type":"s3","endpoint":"a:9000","access-key":"AK","secret-key":"SK","bucket":"b"},
			{"type":"s3","endpoint":"b:9000","access-key":"AK","secret-key":"SK","bucket":"b"}]}`,
		"mirror-file style": `{"quorum":2,"prefix":"svn-ext","mirrors":[
			{"type":"s3","endpoint":"a:9000","access":"AK","secret":"SK","bucket":"b"},
			{"type":"s3","endpoint":"b:9000","access":"AK","secret":"SK","bucket":"b"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			spec, err := Parse(write(t, dir, strings.ReplaceAll(name, " ", "-")+".json", body))
			if err != nil {
				t.Fatal(err)
			}
			if len(spec.Members) != 2 {
				t.Fatalf("members=%d", len(spec.Members))
			}
			if spec.Quorum != 2 {
				t.Fatalf("quorum=%d", spec.Quorum)
			}
			if spec.Prefix != "svn-ext" {
				t.Fatalf("prefix=%q", spec.Prefix)
			}
			for i, m := range spec.Members {
				ak, sk := m.Creds()
				if ak != "AK" || sk != "SK" {
					t.Fatalf("member %d creds=(%q,%q)", i, ak, sk)
				}
			}
		})
	}
}

// TestParseAcceptsBareArray covers the older array-only form still used in
// existing lab scripts.
func TestParseAcceptsBareArray(t *testing.T) {
	dir := t.TempDir()
	body := `[{"type":"file","root":"/tmp/a"},{"type":"file","root":"/tmp/b"}]`
	spec, err := Parse(write(t, dir, "arr.json", body))
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Members) != 2 || spec.Prefix != "repos" {
		t.Fatalf("members=%d prefix=%q", len(spec.Members), spec.Prefix)
	}
}

// TestStoreRefusesMissingCredentials is the diagnostic guarantee: an S3 member
// without keys must fail locally with a message about credentials, instead of
// sending an anonymous request and reporting Access Denied.
func TestStoreRefusesMissingCredentials(t *testing.T) {
	m := Member{Type: "s3", Endpoint: "10.0.0.1:9000", Bucket: "b"}
	_, err := m.Store()
	if err == nil {
		t.Fatal("expected an error for missing credentials")
	}
	if !strings.Contains(err.Error(), "missing credentials") {
		t.Fatalf("error should name the problem: %v", err)
	}
	if strings.Contains(err.Error(), "Access Denied") {
		t.Fatalf("error should not be a server response: %v", err)
	}
}

func TestStoreRefusesSingleBackend(t *testing.T) {
	dir := t.TempDir()
	body := `{"mirrors":[{"type":"file","root":"/tmp/only"}]}`
	if _, err := Parse(write(t, dir, "one.json", body)); err == nil {
		t.Fatal("a mirror of one backend is not a mirror")
	}
}

func TestRepoPrefixTrim(t *testing.T) {
	s := &Spec{Prefix: "/svn-ext/"}
	if got := s.RepoPrefix("app"); got != "svn-ext/app" {
		t.Fatalf("got %q", got)
	}
}
