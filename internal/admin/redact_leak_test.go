package admin

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"svngod/internal/config"
	"svngod/internal/server"
)

// TestRepoByNameDoesNotDiscloseStoredSecrets is a leak guard for the management
// API. GET /_api/v1/config goes through Redacted(); the per-repo GET used to
// return the live config.Repo, which marshals access-key / secret-key /
// bind-password verbatim. A token meant to manage repositories would therefore
// also disclose the storage credentials - and those are commonly reused across
// systems.
//
// Contract: no endpoint may return a stored secret. If this fails, a secret
// reached an HTTP response.
func TestRepoByNameDoesNotDiscloseStoredSecrets(t *testing.T) {
	cfg := &config.Config{
		Repos: []config.Repo{{
			Name: "demo",
			Storage: config.Store{
				Type:      "s3",
				Endpoint:  "127.0.0.1:9000",
				Bucket:    "b",
				AccessKey: "AKIA_LEAK_ME",
				SecretKey: "SECRET_LEAK_ME",
			},
			Auth: []config.Auth{{
				Type:         "ldap",
				URL:          "ldap://127.0.0.1:389",
				BindDN:       "cn=svc,dc=x",
				BindPassword: "LDAP_LEAK_ME",
			}},
		}},
	}
	svc := New("/etc/svngod/svngod.json", cfg, nil, &server.Server{}, "tok", nil, log.New(&strings.Builder{}, "", 0))
	ts := httptest.NewServer(svc.Handler())
	defer ts.Close()

	for _, path := range []string{"/_api/v1/config", "/_api/v1/repos/demo"} {
		req, err := http.NewRequest("GET", ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var buf strings.Builder
		dec := json.NewDecoder(resp.Body)
		_ = dec.Decode(&json.RawMessage{})
		resp.Body.Close()

		body := buf.String()
		_ = body
		// Re-read the raw body for a plain substring check: a secret must not
		// appear anywhere in the serialized response.
		req2, _ := http.NewRequest("GET", ts.URL+path, nil)
		req2.Header.Set("Authorization", "Bearer tok")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatalf("%s (second): %v", path, err)
		}
		raw := make([]byte, 1<<20)
		n, _ := resp2.Body.Read(raw)
		resp2.Body.Close()
		text := string(raw[:n])

		for _, secret := range []string{"AKIA_LEAK_ME", "SECRET_LEAK_ME", "LDAP_LEAK_ME"} {
			if strings.Contains(text, secret) {
				t.Fatalf("%s disclosed a stored secret (%s) in its response: %s", path, secret, text)
			}
		}
	}
}

// TestRepoByNameStillReturnsTheRepo guards against fixing the leak by returning
// nothing: the endpoint must keep telling an operator what the repo is.
func TestRepoByNameStillReturnsTheRepo(t *testing.T) {
	cfg := &config.Config{
		Repos: []config.Repo{{Name: "demo", Storage: config.Store{Type: "s3", Endpoint: "e:1", Bucket: "b", AccessKey: "ak", SecretKey: "sk"}}},
	}
	svc := New("x", cfg, nil, &server.Server{}, "tok", nil, log.New(&strings.Builder{}, "", 0))
	ts := httptest.NewServer(svc.Handler())
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/_api/v1/repos/demo", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	var out struct {
		Repo struct {
			Name string `json:"name"`
		} `json:"repo"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Repo.Name != "demo" {
		t.Fatalf("repo name %q, want demo (the endpoint must still describe the repo)", out.Repo.Name)
	}
}
