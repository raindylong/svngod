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

echo "CI OK"
