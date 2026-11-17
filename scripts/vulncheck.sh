#!/usr/bin/env bash
# vulncheck.sh — dependency vulnerability scan (govulncheck).
#
# Only vulnerabilities the code can actually reach fail the build.
#
# Why the distinction matters here: govulncheck reports three tiers —
#   symbol results  : a vulnerable function is reachable from our code
#   package results : a vulnerable package is imported
#   module results  : a vulnerable module is in the build list
# Failing on all three would make CI permanently red for reasons nobody can fix
# from this repository. Today's example: golang.org/x/crypto is flagged because
# its openpgp package is "unmaintained, unsafe by design" — svngod does not
# import openpgp at all (it is an indirect dependency of go-ldap, for NTLM), and
# the only fix is a pseudo-version. A gate that cannot go green gets ignored, so
# module-level findings are printed as context and do not fail.
#
# Usage: scripts/vulncheck.sh
#   VULNCHECK_SKIP=1        skip (reported, not silent)
#   VULNCHECK_REQUIRE_TOOL=1 fail instead of skipping when govulncheck is missing
#
# Exit: 0 = no reachable vulnerabilities, 1 = found, 77 = tool unavailable.
set -uo pipefail
cd "$(dirname "$0")/.."

if [ "${VULNCHECK_SKIP:-0}" = "1" ]; then
  echo "vulncheck: skipped (VULNCHECK_SKIP=1)"
  exit 0
fi

if ! command -v govulncheck >/dev/null 2>&1; then
  msg="vulncheck: govulncheck not on PATH"
  hint="install: go install golang.org/x/vuln/cmd/govulncheck@latest"
  if [ "${VULNCHECK_REQUIRE_TOOL:-0}" = "1" ]; then
    echo "$msg ($hint)" >&2
    exit 1
  fi
  echo "$msg - SKIPPED ($hint)" >&2
  exit 77
fi

OUT=$(mktemp "${TMPDIR:-/tmp}/svngod-vuln.XXXXXX")
trap 'rm -f "$OUT"' EXIT

# -show verbose so the report names each vulnerability even when it is not
# reachable; the default output only summarises the counts.
govulncheck -show verbose ./... >"$OUT" 2>&1
rc=$?

if [ "$rc" != "0" ]; then
  # Three outcomes share a non-zero status and must not be conflated:
  #   findings present      -> a real vulnerability, report it
  #   scan produced no verdict -> tool/network failure, do NOT call it a vuln
  #   neither               -> unknown, also not a vulnerability
  # Reporting "REACHABLE VULNERABILITY" for a failed download would send whoever
  # is on call hunting a CVE that does not exist.
  # $OUT is the path of the captured report, not its content: grep the file.
  # (printf '%s\n' "$OUT" | grep ... matched the path, so every failed scan was
  # reported as a vulnerability.)
  if grep -q '^Vulnerability #' "$OUT"; then
    echo "vulncheck: REACHABLE VULNERABILITY" >&2
    sed -n '/=== Symbol Results ===/,$p' "$OUT" | head -60 >&2
    exit 1
  fi
  echo "vulncheck: the scan did not complete (network, vuln DB, or tool error) - not clean, not a finding:" >&2
  tail -20 "$OUT" >&2
  exit 1
fi

# Reachable findings are what matter; report the rest for awareness.
# Sections are cut into variables rather than process substitution: `grep -c` on
# <(...) returned 0 for a section that plainly had one entry, which would have
# under-reported module findings.
symbol_sec=$(sed -n '/=== Symbol Results ===/,/=== Package Results ===/p' "$OUT")
pkg_sec=$(sed -n '/=== Package Results ===/,/=== Module Results ===/p' "$OUT")
mod_sec=$(sed -n '/=== Module Results ===/,$p' "$OUT")

count_hits() {
  # grep -c exits 1 on no match, which under `set -e`/`|| true` is easy to
  # mistake for "0"; count explicitly instead.
  printf '%s\n' "$1" | grep -c '^Vulnerability #' || true
}

reachable=$(count_hits "$symbol_sec")
pkg_hits=$(count_hits "$pkg_sec")
mod_hits=$(count_hits "$mod_sec")

echo "vulncheck: reachable=$reachable packages=$pkg_hits modules=$mod_hits"

if [ "$pkg_hits" != "0" ]; then
  # Imported-but-not-called: not a build breaker here, but worth surfacing since
  # it is one refactor away from being reachable.
  echo "vulncheck: warnings in imported packages (not called):" >&2
  sed -n '/=== Package Results ===/,/=== Module Results ===/p' "$OUT" \
    | grep -E '^Vulnerability|^    [A-Z]' | head -20 >&2
fi

echo "VULNCHECK OK (reachable=0)"