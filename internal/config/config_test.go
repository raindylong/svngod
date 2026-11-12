package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveSecret(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "secret")
	if err := os.WriteFile(keyFile, []byte("  s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("inline value", func(t *testing.T) {
		s := &Store{SecretKey: "abc"}
		got, err := s.resolveSecret()
		if err != nil || got != "abc" {
			t.Fatalf("got %q err %v", got, err)
		}
	})

	t.Run("file value is trimmed", func(t *testing.T) {
		s := &Store{SecretKeyFile: keyFile}
		got, err := s.resolveSecret()
		if err != nil || got != "s3cr3t" {
			t.Fatalf("got %q err %v", got, err)
		}
	})

	t.Run("both sources conflict", func(t *testing.T) {
		s := &Store{SecretKey: "abc", SecretKeyFile: keyFile}
		if _, err := s.resolveSecret(); err == nil ||
			!strings.Contains(err.Error(), "not both") {
			t.Fatalf("want not-both error, got %v", err)
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		s := &Store{SecretKeyFile: filepath.Join(dir, "nope")}
		if _, err := s.resolveSecret(); err == nil {
			t.Fatal("want error for missing secret-key-file")
		}
	})
}

func TestScrubIntervalParsing(t *testing.T) {
	var c Config
	if err := json.Unmarshal([]byte(`{"scrub-interval": "10m"}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.ScrubInterval.Std() != 10*time.Minute {
		t.Fatalf("got %v", c.ScrubInterval.Std())
	}
	if err := json.Unmarshal([]byte(`{"scrub-interval": 3600}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.ScrubInterval.Std() != time.Hour {
		t.Fatalf("got %v", c.ScrubInterval.Std())
	}
	c = Config{}
	if err := json.Unmarshal([]byte(`{}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.ScrubInterval.Std() != 0 {
		t.Fatalf("unset should be 0, got %v", c.ScrubInterval.Std())
	}
}
