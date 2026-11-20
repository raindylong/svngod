package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// Redacted() is the only thing standing between the management API and stored
// credentials. It must cover every field that can hold one - including stores
// nested inside geo (Local/Remote) and sharded (ShardMeta/ShardRanges) specs,
// which carry their own S3 keys.
//
// A geo/sharded repo is rejected by Validate() today, but a config file that
// contains one is still loaded and handed to the admin service when the repo
// fails to build, so the nested secrets are reachable through /_api/v1/config.
func TestRedactedCoversNestedStoreSecrets(t *testing.T) {
	cfg := &Config{Repos: []Repo{{
		Name: "g",
		Storage: Store{
			Type: "geo",
			Local: &Store{
				Type:    "mirror",
				Mirrors: []Store{{Type: "s3", AccessKey: "AK_LOCAL", SecretKey: "SK_LOCAL"}},
			},
			Remote: &Store{
				Type:      "s3",
				AccessKey: "AK_REMOTE",
				SecretKey: "SK_REMOTE",
			},
			ShardMeta: &Store{Type: "s3", AccessKey: "AK_META", SecretKey: "SK_META"},
			ShardRanges: []ShardRange{{
				Store: &Store{Type: "s3", AccessKey: "AK_RANGE", SecretKey: "SK_RANGE"},
			}},
		},
	}}}

	b, err := json.Marshal(cfg.Redacted())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	text := string(b)
	for _, secret := range []string{"AK_LOCAL", "SK_LOCAL", "AK_REMOTE", "SK_REMOTE", "AK_META", "SK_META", "AK_RANGE", "SK_RANGE"} {
		if strings.Contains(text, secret) {
			t.Fatalf("Redacted() left a nested secret (%s) in:\n%s", secret, text)
		}
	}
}
