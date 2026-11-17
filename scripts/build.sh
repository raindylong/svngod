#!/usr/bin/env bash
# build.sh — build the three binaries with version metadata stamped in.
#
# Without this, `--version` falls back to the toolchain's VCS stamp, which is
# better than nothing but reports a pseudo-version like
# v0.0.0-20261117073222-0f9cdcb0593f+dirty. A release should say 1.2.3.
#
# Usage:
#   scripts/build.sh [version] [outdir]
#   VERSION=1.2.3 scripts/build.sh
#
# Defaults: version from `git describe --tags --always --dirty`, output to ./bin.
# The date is the COMMIT time, not now: a rebuild of the same commit producing a
# different timestamp makes two identical binaries look like different builds.
set -euo pipefail
cd "$(dirname "$0")/.."

MODULE=svngod
PKG=$MODULE/internal/version

VERSION=${1:-${VERSION:-}}
OUTDIR=${2:-${OUTDIR:-bin}}

if [ -z "$VERSION" ]; then
  if git rev-parse --git-dir >/dev/null 2>&1; then
    VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo dev)
  else
    VERSION=dev
  fi
fi

COMMIT=""
BUILD_DATE=""
if git rev-parse --git-dir >/dev/null 2>&1; then
  COMMIT=$(git rev-parse --short=12 HEAD 2>/dev/null || true)
  # %cI is the committer date in strict ISO 8601; fall back to now if absent.
  BUILD_DATE=$(git log -1 --format=%cI 2>/dev/null || true)
fi
[ -n "$BUILD_DATE" ] || BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS="-X $PKG.Version=$VERSION"
[ -n "$COMMIT" ] && LDFLAGS="$LDFLAGS -X $PKG.Commit=$COMMIT"
LDFLAGS="$LDFLAGS -X $PKG.BuildDate=$BUILD_DATE"

mkdir -p "$OUTDIR"
# CGO_ENABLED=0 everywhere: the release artifacts must be static (see README).
for cmd in svngod svnadmin svngocli; do
  CGO_ENABLED=0 go build -trimpath -ldflags "$LDFLAGS" -o "$OUTDIR/$cmd" "./cmd/$cmd"
done

echo "built version=$VERSION commit=${COMMIT:-none} date=$BUILD_DATE -> $OUTDIR"
"$OUTDIR/svngod" --version