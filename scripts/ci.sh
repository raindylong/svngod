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

# Race detector. The default suite runs with CGO_ENABLED=0 (the release build is
# pure Go, static), and the race detector requires cgo - so this cannot be folded
# into the run above. It is worth a separate pass: the fs and server packages have
# real concurrency (group commit, mirror repair, transaction cleanup) and a race
# there corrupts data rather than producing a wrong answer.
#
# ~20s on a 16-core box. Set CI_SKIP_RACE=1 only for a deliberately quick loop.
if [ "${CI_SKIP_RACE:-0}" != "1" ]; then
  echo "==> race detector (CGO_ENABLED=1)"
  if ! command -v "${CC:-gcc}" >/dev/null 2>&1 && ! command -v clang >/dev/null 2>&1; then
    fail "CGO_ENABLED=1 requires a C compiler; install gcc or clang (or set CI_SKIP_RACE=1)"
  fi
  # "|| rc=$?" is required: under set -e a plain assignment takes the exit status
  # of the command substitution and aborts the script here, so the failure branch
  # below would never run and CI would die without printing why.
  race_rc=0
  race_out=$(CGO_ENABLED=1 go test -race -count=1 -timeout 20m ./... 2>&1) || race_rc=$?
  if [ "$race_rc" != "0" ]; then
    # The report is the product here: print it whole rather than a tail, because
    # the two stacks that matter may be far apart in the output.
    # No pipe into `head` here: closing the pipe early kills the producer with
    # SIGPIPE and CI reports 141 instead of the real failure. Capture, then slice.
    race_report=$(printf '%s\n' "$race_out" | grep -A30 "WARNING: DATA RACE" || true)
    printf '%s\n' "$race_report" | sed -n '1,60p'
    printf '%s\n' "$race_out" | tail -20
    fail "race detector found a data race (see above)"
  fi
  echo "    $(printf '%s\n' "$race_out" | grep -cE '^ok  ') packages clean under -race"
fi

# Fuzzing, short. The corpus committed under testdata/fuzz is what makes this
# cheap: each entry is a past failure, so the seed pass alone re-runs every bug
# found so far. The timed pass is a new-input search and is kept short because
# it is unbounded work - FUZZTIME=0s skips it and only replays the corpus.
#
# This found three real defects in the svndiff decoders (a slice-bounds panic, a
# wrapped length that defeated a bounds check, and an unbounded forward-copy
# loop that hung the process) plus a format round-trip that silently changed a
# repository's storage mode.
if [ "${CI_SKIP_FUZZ:-0}" != "1" ]; then
  echo "==> fuzz (corpus replay + ${FUZZTIME:-20s} search)"
  fuzz_fail=""
  for target in FuzzApply FuzzApplyStream FuzzVersionAndIsDelta; do
    out=$(CGO_ENABLED=0 go test -run "^\$target\$" ./internal/svndiff/ 2>&1) || fuzz_fail="$fuzz_fail $target(corpus)"
  done
  for target in FuzzParseSkel FuzzParseSkelNoHang; do
    out=$(CGO_ENABLED=0 go test -run "^\$target\$" ./internal/server/ 2>&1) || fuzz_fail="$fuzz_fail $target(corpus)"
  done
  for target in FuzzUnmarshalFormat FuzzUnmarshalFormatKeepsUnknownKeys; do
    out=$(CGO_ENABLED=0 go test -run "^\$target\$" ./internal/fs/ 2>&1) || fuzz_fail="$fuzz_fail $target(corpus)"
  done
  if [ -n "$fuzz_fail" ]; then
    echo "    corpus replay failed:$fuzz_fail"
    for t in $fuzz_fail; do
      name=${t%%(*}
      case "$name" in
        FuzzApply|FuzzApplyStream|FuzzVersionAndIsDelta) pkg=./internal/svndiff/ ;;
        FuzzParseSkel*) pkg=./internal/server/ ;;
        *) pkg=./internal/fs/ ;;
      esac
      CGO_ENABLED=0 go test -run "^\$name\$" "$pkg" 2>&1 | tail -15 | sed 's/^/    /'
    done
    fail "fuzz corpus replay failed"
  fi
  if [ "${FUZZTIME:-20s}" != "0s" ]; then
    # One target per package keeps this bounded; the corpus is the real gate.
    CGO_ENABLED=0 go test -run XXX -fuzz '^FuzzApply$' -fuzztime "${FUZZTIME:-20s}" ./internal/svndiff/ \
      >/tmp/svngod-fuzz.$$ 2>&1 || { tail -20 /tmp/svngod-fuzz.$$ | sed 's/^/    /'; fail "fuzzing found a new failure"; }
  fi
  echo "    corpus clean, no new failures"
fi

# Dependency vulnerabilities. CI is the place this must not be skippable: a
# local run without govulncheck installs is reported as skipped, but in CI a
# missing tool is a failure (VULNCHECK_REQUIRE_TOOL=1), otherwise the gate
# silently disappears on a runner image that happens to lack it.
if [ "${CI_SKIP_VULNCHECK:-0}" != "1" ]; then
  echo "==> govulncheck"
  vuln_rc=0
  VULNCHECK_REQUIRE_TOOL="${VULNCHECK_REQUIRE_TOOL:-0}" \
    bash scripts/vulncheck.sh >/tmp/svngod-vuln.$$ 2>&1 || vuln_rc=$?
  if [ "$vuln_rc" = "77" ]; then
    echo "    skipped: $(tail -1 /tmp/svngod-vuln.$$)"
  elif [ "$vuln_rc" != "0" ]; then
    tail -30 /tmp/svngod-vuln.$$ | sed 's/^/    /'
    fail "dependency vulnerabilities found (see above)"
  else
    grep -E '^vulncheck: reachable' /tmp/svngod-vuln.$$ | sed 's/^/    /'
  fi
fi

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
  # Same set -e trap as the race step: without "|| rc=$?" a non-zero exit (77 for
  # "svn not installed") aborts CI silently instead of being handled below.
  svn_rc=0
  svn_out=$(bash scripts/svn-client.sh 2>&1) || svn_rc=$?
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
