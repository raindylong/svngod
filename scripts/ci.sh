#!/usr/bin/env bash
# svngod CI gate — runnable locally (any OS) and in any CI runner.
# Checks: gofmt clean, go vet (linux + darwin), static builds for both
# target platforms, unit tests on the native platform.
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "CI FAIL: $1" >&2; exit 1; }

echo "==> gofmt"
unformatted=$(gofmt -l cmd internal)
if [ -n "$unformatted" ]; then
  echo "$unformatted"
  fail "run: gofmt -w cmd internal"
fi

echo "==> go vet (linux)"
GOOS=linux go vet ./...

echo "==> go vet (darwin)"
GOOS=darwin go vet ./...

echo "==> build (linux, CGO_ENABLED=0)"
CGO_ENABLED=0 GOOS=linux   go build -o /dev/null ./cmd/svngod ./cmd/svnadmin ./cmd/svngocli

echo "==> build (darwin, CGO_ENABLED=0)"
CGO_ENABLED=0 GOOS=darwin  go build -o /dev/null ./cmd/svngod ./cmd/svnadmin ./cmd/svngocli

echo "==> test (native)"
CGO_ENABLED=0 go test -count=1 -timeout 15m ./...

# Backup/restore drill: proves the recovery runbook (docs/备份与恢复SOP.md) on
# a two-node WAL mirror, including the negative controls that must refuse a
# damaged backup. Fast (~5s); set CI_SKIP_DRILL=1 for a unit-tests-only run.
if [ "${CI_SKIP_DRILL:-0}" != "1" ]; then
  echo "==> restore drill"
  bash scripts/drill-restore.sh 40 4 >/tmp/svngod-drill.$$ 2>&1 \
    || { tail -30 /tmp/svngod-drill.$$; fail "restore drill failed (full log: /tmp/svngod-drill.$$)"; }
  grep -q 'RESTORE DRILL OK' /tmp/svngod-drill.$$ \
    || { tail -30 /tmp/svngod-drill.$$; fail "restore drill did not report OK"; }
  tail -1 /tmp/svngod-drill.$$ | sed 's/^/    /'
fi

# Real Subversion client: the unit tests build DAV requests by hand, so they
# cannot show that ra_serf accepts the answers. Set CI_SKIP_SVN_CLIENT=1 for a
# unit-tests-only run. Exit 77 means svn is not installed - reported, not hidden.
if [ "${CI_SKIP_SVN_CLIENT:-0}" != "1" ]; then
  echo "==> svn client"
  svn_out=$(bash scripts/svn-client.sh 2>&1); svn_rc=$?
  if [ "$svn_rc" = "77" ]; then
    echo "    $svn_out" | tail -1
  elif [ "$svn_rc" != "0" ]; then
    printf '%s\n' "$svn_out" | tail -30
    fail "svn client regression failed"
  else
    # Print the verdict line, not the last line: the script's cleanup trap
    # reports artifact paths after the result.
    printf '%s\n' "$svn_out" | grep -E '^SVN-CLIENT OK' | sed 's/^/    /'
  fi
fi

echo "CI OK"
