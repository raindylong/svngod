#!/usr/bin/env bash
# svn-client.sh — end-to-end regression with the real Subversion client.
#
# The server-side tests build DAV requests by hand, which proves svngod answers
# the shapes those tests assert on. It does not prove that ra_serf accepts the
# answers: capability negotiation, the HTTPv2 transaction channel, skel-encoded
# reports and blob/delta ordering are all things a real client exercises and a
# hand-built request cannot.
#
# So this script drives the actual `svn` binary: checkout, commit, update, log,
# blame, diff, plus a branch/copy round trip. Every step asserts on output, not
# just on exit status - a server that answers 200 with an empty body passes an
# exit-status check and fails a user.
#
# Usage: scripts/svn-client.sh
#   SVN_CLIENT_SKIP=1   skip (CI prints why rather than passing silently)
#
# Exit: 0 = every client command produced the expected result.
set -uo pipefail
cd "$(dirname "$0")/.."

fail() { echo "SVN-CLIENT FAIL: $1" >&2; exit 1; }
step() { echo "==> $1"; }

if [ "${SVN_CLIENT_SKIP:-0}" = "1" ]; then
  echo "svn-client: skipped (SVN_CLIENT_SKIP=1)"
  exit 0
fi
if ! command -v svn >/dev/null 2>&1; then
  # Not a pass: say so, and let the caller decide. Silently succeeding would be
  # the same class of mistake as a test that asserts nothing.
  echo "svn-client: SKIPPED - no svn binary on PATH (install subversion to run this)" >&2
  exit 77
fi

SVN="svn --non-interactive --no-auth-cache"
VER=$(svn --version --quiet 2>/dev/null || echo unknown)
echo "svn-client: using svn $VER"

WORK=$(mktemp -d "${TMPDIR:-/tmp}/svngod-svncli.XXXXXX")
# Pick a free port: a fixed one collides with a parallel run or a leftover
# process, and the failure then looks like a server bug. Deriving it from the
# PID keeps this dependency-free; the readiness loop below is what actually
# proves the server is listening.
PORT=${SVN_CLIENT_PORT:-$(( 20000 + ($$ % 20000) ))}
BASE=http://127.0.0.1:$PORT/svn
REPO=demo
SRV_PID=""
cleanup() {
  if [ -n "$SRV_PID" ] && kill -0 "$SRV_PID" 2>/dev/null; then
    kill "$SRV_PID" 2>/dev/null
    wait "$SRV_PID" 2>/dev/null
  fi
  echo "svn-client: artifacts in $WORK"
}
trap cleanup EXIT

step "build"
CGO_ENABLED=0 go build -o "$WORK/svngod" ./cmd/svngod || fail "build svngod"
CGO_ENABLED=0 go build -o "$WORK/svnadmin" ./cmd/svnadmin || fail "build svnadmin"

step "create repository and start server"
"$WORK/svnadmin" create "$WORK/repos/$REPO" >/dev/null 2>&1 || fail "svnadmin create"
"$WORK/svngod" -addr "127.0.0.1:$PORT" -base-path /svn -repo-root "$WORK/repos" \
  >"$WORK/server.log" 2>&1 &
SRV_PID=$!

# Wait for readiness through the same verb the client uses first.
ready=0
for _ in $(seq 1 60); do
  code=$(curl -sS -o /dev/null -m 2 -w '%{http_code}' -X OPTIONS "$BASE/$REPO" 2>/dev/null || echo 000)
  [ "$code" = "200" ] && { ready=1; break; }
  kill -0 "$SRV_PID" 2>/dev/null || break
  sleep 0.25
done
[ "$ready" = "1" ] || { tail -20 "$WORK/server.log"; fail "server did not become ready"; }

WC="$WORK/wc"

step "checkout (empty repository)"
$SVN checkout -q "$BASE/$REPO" "$WC" || fail "checkout"
[ -d "$WC/.svn" ] || fail "checkout produced no working copy metadata"

step "add and commit"
mkdir -p "$WC/trunk"
printf 'line one\nline two\n' > "$WC/trunk/a.txt"
printf 'second file\n' > "$WC/trunk/b.txt"
( cd "$WC" && $SVN add -q trunk ) || fail "svn add"
( cd "$WC" && $SVN commit -q -m "initial import" ) || fail "commit"

# The commit must be revision 1 and visible to the server, not just to the
# working copy: an accepted commit that never advanced HEAD is the exact failure
# a client-only assertion would miss.
#
# Note: `svn info` on the working copy root still says 0 here, because the commit
# touched trunk/ and the root itself did not change. That is normal client
# behaviour, not a server fault, so the assertion is on the server's HEAD.
head=$("$WORK/svnadmin" info "$WORK/repos/$REPO" | sed -n 's/^head: //p')
[ "$head" = "1" ] || fail "server HEAD is '$head' after the first commit, want 1"

step "log"
# Update first: the working copy root is still at r0 because the commit touched
# trunk/, and `svn log` on a root pinned to r0 reports nothing.
( cd "$WC" && $SVN update -q ) || fail "update before log"
log_out=$( ( cd "$WC" && $SVN log ) ) || fail "svn log"
printf '%s' "$log_out" | grep -q "initial import" || fail "log does not show the commit message"
printf '%s' "$log_out" | grep -q "^r1 |" || fail "log does not show r1"
# -v must name the changed paths; an empty changed-path list is a report bug
# that a plain `svn log` would not reveal.
log_v=$( ( cd "$WC" && $SVN log -v ) ) || fail "svn log -v"
printf '%s' "$log_v" | grep -q "A /trunk/a.txt" || fail "log -v does not report /trunk/a.txt as added"

step "update after a second commit"
printf 'line one\nline two\nline three\n' > "$WC/trunk/a.txt"
( cd "$WC" && $SVN commit -q -m "add third line" ) || fail "second commit"
other="$WORK/wc2"
$SVN checkout -q "$BASE/$REPO" "$other" || fail "second checkout"
grep -q "line three" "$other/trunk/a.txt" || fail "fresh checkout does not see the second commit"
# Now move the first working copy forward and confirm the text arrives.
( cd "$WC" && $SVN update -q ) || fail "update"
grep -q "line three" "$WC/trunk/a.txt" || fail "update did not deliver the new line"

step "blame"
# Attribution is the interesting part: r1 lines must stay r1, the new line is r2.
blame=$( ( cd "$WC" && $SVN blame trunk/a.txt ) ) || fail "svn blame"
printf '%s\n' "$blame" | grep -qE '^ +1 .*line one' || fail "blame does not attribute 'line one' to r1: $blame"
printf '%s\n' "$blame" | grep -qE '^ +2 .*line three' || fail "blame does not attribute 'line three' to r2: $blame"

step "diff"
diff_out=$( ( cd "$WC" && $SVN diff -c 2 ) ) || fail "svn diff -c 2"
printf '%s' "$diff_out" | grep -q "^+line three" || fail "diff does not show the added line"
printf '%s' "$diff_out" | grep -q "revision 1" || fail "diff does not reference the base revision"

step "list and cat"
( cd "$WC" && $SVN list ) | grep -qx "trunk/" || fail "ls does not show trunk/"
( cd "$WC" && $SVN cat trunk/a.txt ) | grep -q "line three" || fail "cat returned stale content"

step "copy (branch) round trip"
( cd "$WC" && $SVN copy -q -m "branch trunk" "$BASE/$REPO/trunk" "$BASE/$REPO/branches/trunk-b" ) \
  || fail "server-side copy"
( cd "$WC" && $SVN update -q ) || fail "update after copy"
[ -f "$WC/branches/trunk-b/a.txt" ] || fail "copied branch is not in the working copy"
grep -q "line three" "$WC/branches/trunk-b/a.txt" || fail "copied branch has wrong content"

step "delete and commit"
( cd "$WC" && $SVN delete -q trunk/b.txt && $SVN commit -q -m "drop b.txt" ) || fail "delete + commit"
( cd "$WC" && $SVN update -q ) || fail "update after delete"
[ ! -f "$WC/trunk/b.txt" ] || fail "deleted file still present after update"

step "final repository state"
final_log=$( ( cd "$WC" && $SVN log -q ) )
count=$(printf '%s\n' "$final_log" | grep -c '^r[0-9]' || true)
[ "$count" = "4" ] || fail "expected 4 revisions, found $count"

# Server-side check: the client saw what it saw, but the repository has to agree.
head=$("$WORK/svnadmin" info "$WORK/repos/$REPO" 2>/dev/null | sed -n 's/^head: //p')
[ "$head" = "4" ] || fail "server HEAD is '$head', want 4"

echo "SVN-CLIENT OK (svn $VER, 4 revisions, checkout/commit/update/log/blame/diff/copy/delete)"