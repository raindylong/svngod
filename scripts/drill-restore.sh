#!/usr/bin/env bash
# drill-restore.sh — backup/restore drill for svngod.
#
# Runs the full recovery chain end to end and fails on the first broken link:
#
#   seed repo (WAL mirror) -> dump --verify -> authenticate sha256
#   -> load --expect-head into a fresh mirror -> verify --all-revs --texts
#      (tree + every text + WORM journal hash chain)
#   -> dump again -> byte-identical to the first dump (idempotence)
#
# Then proves the gates actually refuse a bad backup:
#   - a rewritten journal record must abort `dump --verify` (hash chain)
#   - a corrupted HEAD rev-block must abort `dump --verify` (readability)
#   - `load --expect-head` must reject a wrong revision count
#
# The mirror backend is used (two local directories, quorum 2) because the
# journal hash chain only exists where WAL runs; a plain file store has no
# journal, so a chain check there would silently pass and prove nothing.
#
# Usage: scripts/drill-restore.sh [revisions] [workers]
#        DRILL_MIRROR_FILE=/path/mirror.json scripts/drill-restore.sh [revs]
#
# DRILL_MIRROR_FILE points the positive chain at an external mirror spec (for
# example the two-instance S3 lab) instead of two local directories. The
# negative controls always run on local directories: they exercise the gates,
# not the backend, and must be able to corrupt objects directly.
#
# Exit: 0 = every recovery assumption held, non-zero = a link is broken.
set -euo pipefail
cd "$(dirname "$0")/.."

REVS=${1:-40}
# One identity per run: a reused repository name would seed on top of a previous
# run (head carries over) and the restored UUID could never match, making the
# drill's verdict depend on history instead of on this run's backup.
RUN_ID=$(date -u +%Y%m%dT%H%M%S)-$$
REPO=drill-$RUN_ID
WORKERS=${2:-4}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/svngod-drill.XXXXXX")
trap 'echo "artifacts: $WORK"' EXIT

step() { printf '\n==> %s\n' "$1"; }
fail() { echo "DRILL FAIL: $1" >&2; exit 1; }

# mirrorSpec <dir-a> <dir-b> <json-path> — quorum 2 over two local directories.
mirrorSpec() {
  cat > "$3" <<EOF
{"quorum":2,"prefix":"repos","mirrors":[{"type":"file","root":"$1"},{"type":"file","root":"$2"}]}
EOF
}

step "build svnadmin + revscale"
CGO_ENABLED=0 go build -o "$WORK/svnadmin" ./cmd/svnadmin
CGO_ENABLED=0 go build -o "$WORK/revscale" ./cmd/revscale
SVNADMIN="$WORK/svnadmin"

if [ -n "${DRILL_MIRROR_FILE:-}" ]; then
  step "seed a repository with $REVS revisions (external mirror: $DRILL_MIRROR_FILE)"
  [ -r "$DRILL_MIRROR_FILE" ] || fail "DRILL_MIRROR_FILE is not readable: $DRILL_MIRROR_FILE"
  # Keep the caller's spec untouched: copy it with this run's prefix so the seed
  # starts empty and cannot accumulate across runs.
  SRC_MIRROR="$WORK/src-ext.json"
  python3 - "$DRILL_MIRROR_FILE" "$SRC_MIRROR" "drill-src-$RUN_ID" <<'PYEOF'
import json, sys
src, dst, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
spec = json.load(open(src))
spec["prefix"] = prefix
json.dump(spec, open(dst, "w"))
PYEOF
  "$SVNADMIN" create --mirror-file "$SRC_MIRROR" "$REPO" >/dev/null
else
  step "seed a repository with $REVS revisions (two-node WAL mirror, quorum 2)"
  mkdir -p "$WORK/src1" "$WORK/src2"
  mirrorSpec "$WORK/src1" "$WORK/src2" "$WORK/src.json"
  SRC_MIRROR="$WORK/src.json"
  "$SVNADMIN" create --mirror-file "$SRC_MIRROR" "$REPO" >/dev/null
fi
# -mixed (adds/modifies/deletes/copies) needs a single writer: its generator
# assumes sequential state, so concurrent deletes of the same path abort
# commits. The drill wants a predictable seed, not throughput.
if [ -n "${DRILL_MIRROR_FILE:-}" ]; then
  SEED_LOG=$("$WORK/revscale" -store mirrorfile \
    -mirror-file "$SRC_MIRROR" -repo "$REPO" \
    -target "$REVS" -workers 1 -mixed -progress 1m 2>&1)
else
  SEED_LOG=$("$WORK/revscale" -store mirrorfile -endpoints "$WORK/src1,$WORK/src2" \
    -repo "$REPO" -target "$REVS" -workers 1 -mixed -progress 1m 2>&1)
fi
echo "$SEED_LOG" | tail -2
if echo "$SEED_LOG" | grep -q ' ERR '; then
  fail "seeding aborted a commit (see log above)"
fi
HEAD_SEED=$("$SVNADMIN" info --mirror-file "$SRC_MIRROR" "$REPO" | awk '/^head:/{print $2}')
[ -n "${HEAD_SEED:-}" ] || fail "could not read seeded HEAD"
[ "$HEAD_SEED" -ge "$REVS" ] || fail "seeded only $HEAD_SEED of $REVS revisions"
echo "seeded head=$HEAD_SEED"

step "backup: dump --verify (refuses to export corrupt history)"
"$SVNADMIN" dump --mirror-file "$SRC_MIRROR" --verify --output "$WORK/app.dump" "$REPO" \
  2> "$WORK/dump.log"
grep -v '^dump: repo=' "$WORK/dump.log" || true
cat "$WORK/dump.log" | grep '^dump: repo=' || fail "dump produced no manifest"
MANIFEST_HEAD=$(awk '/^dump: repo=/{for(i=1;i<=NF;i++) if($i ~ /^head=/) {sub("head=","",$i); print $i}}' "$WORK/dump.log")
MANIFEST_SHA=$(awk '/^dump: repo=/{for(i=1;i<=NF;i++) if($i ~ /^sha256=/) {sub("sha256=","",$i); print $i}}' "$WORK/dump.log")
MANIFEST_BYTES=$(awk '/^dump: repo=/{for(i=1;i<=NF;i++) if($i ~ /^bytes=/) {sub("bytes=","",$i); print $i}}' "$WORK/dump.log")
[ "$MANIFEST_HEAD" = "$HEAD_SEED" ] || fail "manifest head $MANIFEST_HEAD != seeded head $HEAD_SEED"

step "authenticate the backup copy (sha256 + size, as transferred)"
ACTUAL_SHA=$(shasum -a 256 "$WORK/app.dump" | awk '{print $1}')
ACTUAL_BYTES=$(wc -c < "$WORK/app.dump" | tr -d ' ')
[ "$ACTUAL_SHA" = "$MANIFEST_SHA" ] || fail "sha256 mismatch: manifest=$MANIFEST_SHA file=$ACTUAL_SHA"
[ "$ACTUAL_BYTES" = "$MANIFEST_BYTES" ] || fail "size mismatch: manifest=$MANIFEST_BYTES file=$ACTUAL_BYTES"
echo "backup authenticated: $ACTUAL_BYTES bytes, sha256=$ACTUAL_SHA"

step "no .partial file left at the trusted path"
[ ! -f "$WORK/app.dump.partial" ] || fail "dump left a .partial file behind"

step "restore into a fresh mirror, asserting the revision count"
# Each run owns a unique prefix. A reused prefix would restore into the previous
# run's repository: load then skips existing revisions by design and cannot
# adopt the dump's UUID, so the re-dump comparison would fail for a reason that
# has nothing to do with this run's backup.
RUN_ID=$(date -u +%Y%m%dT%H%M%S)-$$
if [ -n "${DRILL_MIRROR_FILE:-}" ]; then
  DST_MIRROR="$WORK/dst-ext.json"
  python3 - "$DRILL_MIRROR_FILE" "$DST_MIRROR" "drill-restore-$RUN_ID" <<'PYEOF'
import json, sys
src, dst, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
spec = json.load(open(src))
spec["prefix"] = prefix
json.dump(spec, open(dst, "w"))
PYEOF
else
  mkdir -p "$WORK/dst1" "$WORK/dst2"
  mirrorSpec "$WORK/dst1" "$WORK/dst2" "$WORK/dst.json"
  DST_MIRROR="$WORK/dst.json"
fi
"$SVNADMIN" load --mirror-file "$DST_MIRROR" --input "$WORK/app.dump" \
  --expect-head "$HEAD_SEED" "$REPO" 2>&1 | tail -2

step "restored repository carries the source UUID"
SRC_UUID=$("$SVNADMIN" info --mirror-file "$SRC_MIRROR" "$REPO" | awk '/^uuid:/{print $2}')
DST_UUID=$("$SVNADMIN" info --mirror-file "$DST_MIRROR" "$REPO" | awk '/^uuid:/{print $2}')
[ -n "$SRC_UUID" ] && [ -n "$DST_UUID" ] || fail "could not read uuids (src=$SRC_UUID dst=$DST_UUID)"
[ "$SRC_UUID" = "$DST_UUID" ] || fail "restored uuid $DST_UUID != source $SRC_UUID"
echo "uuid preserved: $SRC_UUID"

step "restore assertions: tree, every text, WORM journal chain"
"$SVNADMIN" verify --mirror-file "$DST_MIRROR" --all-revs --texts --workers "$WORKERS" "$REPO" \
  2>&1 | tail -3
"$SVNADMIN" journal --mirror-file "$DST_MIRROR" -n 1 "$REPO" | tail -1

step "fingerprint: an independent re-dump must reproduce the backup exactly"
"$SVNADMIN" dump --mirror-file "$DST_MIRROR" --verify --output "$WORK/app2.dump" "$REPO" \
  2> "$WORK/dump2.log"
RESTORED_SHA=$(shasum -a 256 "$WORK/app2.dump" | awk '{print $1}')
[ "$RESTORED_SHA" = "$ACTUAL_SHA" ] || fail "re-dump differs from backup: $RESTORED_SHA != $ACTUAL_SHA"
echo "re-dump identical: $RESTORED_SHA"

# The three controls below corrupt objects on disk, so they only apply when
# the positive chain used local directories. The gates themselves live in
# svnadmin, not in a backend, so local coverage is representative - but say
# so out loud instead of passing quietly.
if [ -n "${DRILL_MIRROR_FILE:-}" ]; then
  step "object-corruption negative controls: skipped (external mirror in use)"
  echo "  rerun without DRILL_MIRROR_FILE to exercise them on local directories"
else
  step "negative control: a damaged WAL record must block the backup"
  cp -a "$WORK/src1" "$WORK/badchain1"
  cp -a "$WORK/src2" "$WORK/badchain2"
  mirrorSpec "$WORK/badchain1" "$WORK/badchain2" "$WORK/bad.json"
  REC=$(printf '%s/badchain1/repos/%s/journal/%012d' "$WORK" "$REPO" 2)
  [ -f "$REC" ] || fail "expected journal record at $REC"
  printf 'journal tampering test\n' > "$REC"
  if "$SVNADMIN" dump --mirror-file "$WORK/bad.json" --verify --output "$WORK/bad.dump" "$REPO" \
        2> "$WORK/bad.log"; then
    fail "dump --verify accepted a damaged journal record"
  fi
  [ ! -f "$WORK/bad.dump" ] || fail "a refused dump still produced a file"
  echo "damaged WAL refused: $(grep -v '^dump: repo=' "$WORK/bad.log" | tail -1)"

  step "negative control: a missing WAL record must be detected as a chain gap"
  cp -a "$WORK/src1" "$WORK/gap1"
  cp -a "$WORK/src2" "$WORK/gap2"
  mirrorSpec "$WORK/gap1" "$WORK/gap2" "$WORK/gap.json"
  # Move the record out of the tree instead of deleting it: the drill only needs
    # the journal hole, and keeping the file makes the artifact inspectable.
    mv "$(printf '%s/repos/%s/journal/%012d' "$WORK/gap1" "$REPO" 2)" "$WORK/gap1-removed-journal-2"
    mv "$(printf '%s/repos/%s/journal/%012d' "$WORK/gap2" "$REPO" 2)" "$WORK/gap2-removed-journal-2"
  if "$SVNADMIN" dump --mirror-file "$WORK/gap.json" --verify --output "$WORK/gap.dump" "$REPO" \
        2> "$WORK/gap.log"; then
    fail "dump --verify accepted a journal with a hole in it"
  fi
  [ ! -f "$WORK/gap.dump" ] || fail "a refused dump still produced a file"
  grep -qi 'gap\|chain' "$WORK/gap.log" \
    || { cat "$WORK/gap.log"; fail "refusal did not name the chain gap"; }
  echo "chain gap refused: $(grep -v '^dump: repo=' "$WORK/gap.log" | tail -1)"

  step "negative control: a corrupted HEAD rev-block must block the backup"
  cp -a "$WORK/src1" "$WORK/badhead1"
  cp -a "$WORK/src2" "$WORK/badhead2"
  mirrorSpec "$WORK/badhead1" "$WORK/badhead2" "$WORK/badhead.json"
  BLOCK=$(find "$WORK/badhead1" "$WORK/badhead2" -path '*revs*' -type f -name "$(printf '%012d' "$HEAD_SEED")" | head -1)
  [ -n "$BLOCK" ] || BLOCK=$(find "$WORK/badhead1" "$WORK/badhead2" -type f -name '*.pack' | head -1)
  [ -n "$BLOCK" ] || fail "could not locate the rev-block for HEAD $HEAD_SEED"
  printf 'corrupted' > "$BLOCK"
  if "$SVNADMIN" dump --mirror-file "$WORK/badhead.json" --verify \
        --output "$WORK/bad2.dump" "$REPO" 2> "$WORK/bad2.log"; then
    fail "dump --verify accepted a corrupted HEAD"
  fi
  [ ! -f "$WORK/bad2.dump" ] || fail "a refused dump still produced a file"
  echo "HEAD corruption refused: $(grep -v '^dump: repo=' "$WORK/bad2.log" | tail -1)"
fi

step "negative control: load --expect-head rejects a wrong revision count"
mkdir -p "$WORK/wrong1" "$WORK/wrong2"
mirrorSpec "$WORK/wrong1" "$WORK/wrong2" "$WORK/wrong.json"
if "$SVNADMIN" load --mirror-file "$WORK/wrong.json" --input "$WORK/app.dump" \
      --expect-head "$((HEAD_SEED + 1))" "$REPO" > /dev/null 2> "$WORK/expect.log"; then
  fail "load --expect-head accepted the wrong revision count"
fi
grep -q 'expected' "$WORK/expect.log" || { cat "$WORK/expect.log"; fail "expect-head refusal gave no diagnostic"; }
echo "expect-head guard works: $(tail -1 "$WORK/expect.log")"

echo
echo "RESTORE DRILL OK (revs=$HEAD_SEED, backup=$ACTUAL_BYTES bytes, sha256=$ACTUAL_SHA)"