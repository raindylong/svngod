#!/bin/bash
# Chaos checks for P1 features: paged directories, mirror read repair,
# cross-replica locks, hook-gated revprop changes. Lab paths match faults.sh.
set -u
export LANG=C
U="--non-interactive --username alice --password secret"
REPO=http://127.0.0.1
fail=0
pass() { echo "PASS $*"; }
bad()  { echo "FAIL $*"; fail=$((fail+1)); }
need() { [ -e "$1" ] || { bad "missing $1"; return 1; }; }

wait_up() { # port repo
  local port=$1 repo=$2
  for i in $(seq 1 60); do
    code=$(curl -sS -m 2 -o /dev/null -w '%{http_code}' -u alice:secret -X OPTIONS \
      "http://127.0.0.1:$port/svn/$repo" 2>/dev/null)
    [ "$code" = "200" ] && return 0
    sleep 0.5
  done
  return 1
}
for pair in "18202 stat1" "18210 stat1" "18211 stat1" "18330 mx3"; do
  set -- $pair
  wait_up "$1" "$2" || { bad "server $1 not ready"; }
done

echo "== A. paged directory corruption is detected"
CNAME=chaosdir-$RANDOM
D=/tmp/svnlab/repos/$CNAME
rm -rf /tmp/svnlab/repos/repos/$CNAME
/tmp/svnadmin-new create --dir-buckets "$D" >/dev/null 2>&1 || { bad "create $CNAME"; }
svn mkdir -q $U -m init $REPO:18202/svn/$CNAME/rel 2>/dev/null || true
/tmp/svngocli-new upload /tmp/many $REPO:18202/svn/$CNAME/rel/big -m chaos --username alice --password secret >/dev/null 2>&1
n=$(svn ls $U $REPO:18202/svn/$CNAME/rel/big 2>/dev/null | wc -l)
[ "$n" -eq 1500 ] && pass "paged dir listing=$n" || bad "paged dir listing=$n"
victim=$(find /tmp/svnlab/repos/repos/$CNAME/dirs -type f 2>/dev/null | head -1)
[ -n "$victim" ] && pass "dir bucket objects present" || bad "no dir bucket objects"
rm -f "$victim"
out=$(/tmp/svnadmin-new verify --all-revs "$D" 2>&1 | tail -1)
echo "$out" | grep -q 'no integrity issues' && bad "missing bucket not detected" || pass "missing bucket detected"
rm -rf /tmp/svnlab/repos/repos/$CNAME

echo "== B. mirror read repair heals the primary"
P=/tmp/s3p/svn/repos/mx3/chunks
S=/tmp/s3s/svn/repos/mx3/chunks
# The most recently written chunk belongs to the current revision, so the
# read will actually need it (and therefore trigger the repair).
victim=""
for b in $(ls -t "$S"); do
  [ -f "$P/$b" ] && { victim="$b"; break; }
done
if [ -z "$victim" ]; then
  bad "no chunk present on both mirrors"
else
  before=$(sha256sum "$P/$victim" | awk '{print $1}')
  rm -f "$P/$victim"
  svn cat $U $REPO:18330/svn/mx3/chunked/big.bin 2>/dev/null | sha256sum | awk '{print $1}' >/tmp/chaos_cat.sha
  want=$(sha256sum /tmp/m100/big.bin | awk '{print $1}')
  [ "$(cat /tmp/chaos_cat.sha)" = "$want" ] && pass "read served while primary object missing" || bad "read hash mismatch"
  healed=0
  for i in $(seq 1 20); do
    if [ -f "$P/$victim" ] && [ "$(sha256sum "$P/$victim" | awk '{print $1}')" = "$before" ]; then healed=$i; break; fi
    sleep 0.5
  done
  [ "$healed" -gt 0 ] && pass "primary healed after ${healed}x0.5s" || bad "read repair did not heal primary"
fi

echo "== C. cross-replica lock visibility"
tag=chaoslock-$RANDOM
rm -rf /tmp/chaoslock; mkdir -p /tmp/chaoslock; echo x > /tmp/chaoslock/a.txt
/tmp/svngocli-new upload /tmp/chaoslock $REPO:18210/svn/stat1/$tag -m chaos --username alice --password secret >/dev/null 2>&1
url10=$REPO:18210/svn/stat1/$tag/a.txt
url11=$REPO:18211/svn/stat1/$tag/a.txt
lockbody='<?xml version="1.0"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:owner>chaos</D:owner></D:lockinfo>'
tok=$(curl -sS -m 5 -u alice:secret -X LOCK -H 'Content-Type: text/xml' --data-binary "$lockbody" "$url10" -D- -o /dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="lock-token"{print $2}')
[ -n "$tok" ] && pass "lock on 18210" || bad "lock on 18210 failed"
code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' -u alice:secret -X LOCK -H 'Content-Type: text/xml' --data-binary "$lockbody" "$url11")
[ "$code" = "423" ] && pass "18211 sees the lock (423)" || bad "18211 LOCK=$code, want 423"
curl -sS -m 5 -o /dev/null -u alice:secret -X UNLOCK -H "Lock-Token: $tok" "$url10"
code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' -u alice:secret -X LOCK -H 'Content-Type: text/xml' --data-binary "$lockbody" "$url11")
[ "$code" = "200" ] && pass "18211 can lock after unlock" || bad "18211 LOCK after unlock=$code"
tok2=$(curl -sS -m 5 -u alice:secret -X LOCK -H 'Content-Type: text/xml' --data-binary "$lockbody" "$url11" -D- -o /dev/null | tr -d '\r' | awk -F': ' 'tolower($1)=="lock-token"{print $2}')
[ -n "$tok2" ] && curl -sS -m 5 -o /dev/null -u alice:secret -X UNLOCK -H "Lock-Token: $tok2" "$url11"

echo "== D. revprop hook"
RNAME=chaosrp-$RANDOM
R=/tmp/svnlab/repos/$RNAME
rm -rf /tmp/svnlab/repos/repos/$RNAME
/tmp/svnadmin-new create "$R" >/dev/null 2>&1
svn mkdir -q $U -m init $REPO:18202/svn/$RNAME/trunk 2>/dev/null || true
if svn propset --revprop -r 1 svn:log 'chaos-edit' $U $REPO:18202/svn/$RNAME >/dev/null 2>&1; then
  pass "propset with hook"
else
  bad "propset with hook"
fi
got=$(svn propget --revprop -r 1 svn:log $U $REPO:18202/svn/$RNAME 2>/dev/null)
[ "$got" = "chaos-edit" ] && pass "propget reflects change" || bad "propget=$got"
if svn propset --revprop -r 1 svn:log 'nope' $U $REPO:18210/svn/$RNAME 2>/tmp/chaos_prop.err; then
  bad "propset without hook unexpectedly succeeded"
else
  grep -q 'E165006' /tmp/chaos_prop.err && pass "no-hook server returns E165006" || bad "no-hook error: $(tail -1 /tmp/chaos_prop.err)"
fi
rm -rf /tmp/svnlab/repos/repos/$RNAME

echo "chaos failures=$fail"
[ "$fail" -eq 0 ]
