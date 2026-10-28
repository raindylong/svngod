package auth

import (
	"crypto/sha1"
	"encoding/base64"
	"os/exec"
	"strings"
	"testing"
)

func haveOpenSSL() bool {
	_, err := exec.LookPath("openssl")
	return err == nil
}

func opensslCrypt(t *testing.T, salt, pw string) string {
	t.Helper()
	out, err := exec.Command("openssl", "passwd", "-6", "-salt", salt, pw).Output()
	if err != nil {
		t.Fatalf("openssl: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func sha1B64(s string) string {
	sum := sha1.Sum([]byte(s))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// TestSha512CryptKnownVector uses the reference test vector from the original
// SHA-crypt suite: password "Hello world!", salt "$6$saltstring".
func TestSha512CryptKnownVector(t *testing.T) {
	const want = "$6$saltstring$svn8UoSVapNtMuq1ukKS4tPQd8iKwSMHWjl/O817G3uBnIFNjnQJuesI68u4OTLiBFdcbYEdFCoEOfaS35inz1"
	if got := sha512Crypt("Hello world!", "saltstring", 5000); got != want {
		t.Fatalf("sha512crypt mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestVerifyPasswordFormats(t *testing.T) {
	const pw = "secret"
	shaRec := "{SHA}" + sha1B64(pw)
	cryptRec := "{CRYPT}" + sha512Crypt(pw, "mysalt", 5000)
	cases := []struct {
		stored string
		submit string
		want   bool
	}{
		{pw, pw, true},
		{pw, "wrong", false},
		{shaRec, pw, true},
		{shaRec, "wrong", false},
		{cryptRec, pw, true},
		{cryptRec, "wrong", false},
		{"{CRYPT}$1$xx$yy", pw, false}, // unsupported crypt must not succeed
		{"$6$salt$hash", pw, false},    // missing {CRYPT} prefix
	}
	for _, c := range cases {
		if got := VerifyPassword(c.stored, c.submit); got != c.want {
			t.Fatalf("VerifyPassword(%q, %q) = %v, want %v", c.stored, c.submit, got, c.want)
		}
	}
	// rounds=N custom salt must parse and verify too.
	full := sha512Crypt("abc123", "xyz", 10000)
	hashPart := full[strings.LastIndex(full, "$")+1:]
	rec := "{CRYPT}$6$rounds=10000$xyz$" + hashPart
	if !VerifyPassword(rec, "abc123") || VerifyPassword(rec, "nope") {
		t.Fatal("rounds= custom crypt failed")
	}
}

// TestSha512CryptCrossOpenSSL compares against openssl for passwords within
// openssl's 256-char limit. Skipped when openssl is unavailable.
func TestSha512CryptCrossOpenSSL(t *testing.T) {
	if !haveOpenSSL() {
		t.Skip("openssl not available")
	}
	salts := []string{"S4q7ojUq", "aa", "0123456789abcdef", "saltstring"}
	lens := []int{1, 2, 8, 12, 15, 16, 31, 32, 63, 64, 65, 66, 71, 72, 73, 100, 127, 128, 129, 200, 256}
	for _, salt := range salts {
		for _, n := range lens {
			pw := strings.Repeat("x", n)
			want := opensslCrypt(t, salt, pw)
			if got := sha512Crypt(pw, salt, 5000); got != want {
				t.Fatalf("len=%d salt=%s\n got=%s\nwant=%s", n, salt, got, want)
			}
		}
	}
}
