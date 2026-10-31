#!/bin/bash
# P0 fault matrix for the S3 primary/standby mirror + two-process commits.
#
# Prerequisites (paths match the local test lab):
#   - Apache (ref) or svngod on 18202 plus a second svngod instance on 18203,
#     both with --repo-root /tmp/svnlab/repos, passwd/authz alice:secret;
#   - a mirror-config svngod on 18330 with repository mx3, backed by two
#     fake-s3 instances on 19001/19002 (roots /tmp/s3p, /tmp/s3s);
#   - binaries: /tmp/svngocli, /tmp/svnadmin-work, /tmp/svngod-work, /tmp/fake-s3.
# Prints PASS/FAIL per check. Run from the lab host.
#!/bin/bash
# P0-2 fault matrix (S3 mirror paths). Prints PASS/FAIL lines.
set -u
export LANG=C
U=http://127.0.0.1:18330/svn/mx3
CLI=/tmp/svngocli
mkfs3() { # port root extra_env log
  local p=$1 root=$2 env=$3 log=$4
  lsof -ti tcp:$p -sTCP:LISTEN 2>/dev/null | xargs -r kill
  sleep 0.3
  setsid env FAKE_S3_ROOT=$root FAKE_S3_ADDR=127.0.0.1:$p $env /tmp/fake-s3 >$log 2>&1 </dev/null &
  sleep 0.4
}
up() { # dir url
  $CLI upload "$1" "$2" -m fault --username alice --password secret 2>/tmp/up.err
  local rc=$?
  if [ $rc -ne 0 ]; then echo "FAIL upload $2: $(tail -1 /tmp/up.err)"; return 1; fi
  return 0
}
rm -rf /tmp/up1k; python3 - <<'PY'
import os
for d in range(10):
    p=f'/tmp/up1k/d{d}'; os.makedirs(p)
    for f in range(100): open(f'{p}/f{f}.txt','w').write(f'{d}/{f}\n')
PY

echo "== A. S3 5% errors"
mkfs3 19001 /tmp/s3p "FAKE_S3_ERROR_PCT=5" /tmp/f_p.log
mkfs3 19002 /tmp/s3s "" /tmp/f_s.log
up /tmp/up1k $U/fault1 && echo "PASS upload-with-5pct-errors" || echo "FAIL upload-with-5pct-errors"
n=$(svn ls -R $U/fault1 --username alice --password secret --non-interactive --no-auth-cache 2>/dev/null | wc -l)
[ "$n" -eq 1010 ] && echo "PASS 5pct-files=$n" || echo "FAIL 5pct-files=$n"

echo "== B. primary failover"
mkfs3 19001 /tmp/s3p "" /tmp/f_p.log
mkfs3 19002 /tmp/s3s "" /tmp/f_s.log
lsof -ti tcp:19001 -sTCP:LISTEN 2>/dev/null | xargs -r kill; sleep 0.4
up /tmp/up1k $U/fault2 && echo "PASS upload-primary-down" || echo "FAIL upload-primary-down"
mkfs3 19001 /tmp/s3p "" /tmp/f_p.log
ok=0
for i in $(seq 1 75); do
  (cd /tmp/s3p/svn/repos/mx3 && find . -type f | sort | xargs sha256sum) >/tmp/hp 2>/dev/null
  (cd /tmp/s3s/svn/repos/mx3 && find . -type f | sort | xargs sha256sum) >/tmp/hs 2>/dev/null
  if diff -q /tmp/hp /tmp/hs >/dev/null 2>&1; then ok=$i; break; fi
  sleep 1
done
[ "$ok" -gt 0 ] && echo "PASS mirrors-converged-after=${ok}s" || echo "FAIL mirrors-converged"

echo "== C. integrity"
/tmp/svnadmin-work verify --mirror-file /tmp/mirror.json /tmp/svnlab/repos/mx3 2>&1 | tail -1
/tmp/svnadmin-work dump --mirror-file /tmp/mirror.json /tmp/svnlab/repos/mx3 >/tmp/mx3.dump 2>/dev/null
rm -rf /tmp/rlfault && svnadmin create /tmp/rlfault && svnadmin load -q /tmp/rlfault </tmp/mx3.dump
if [ $? -eq 0 ]; then echo "PASS dump-load rev=$(svnlook youngest /tmp/rlfault)"; else echo "FAIL dump-load"; fi

echo "== D. two-process disjoint commits (lease)"
for p in 18202 18203; do
  lsof -ti tcp:$p -sTCP:LISTEN 2>/dev/null | xargs -r kill
done
sleep 0.5
for p in 18202 18203; do
  setsid /tmp/svngod-work --addr 127.0.0.1:$p --base-path /svn --repo-root /tmp/svnlab/repos --passwd /tmp/svnlab/passwd --authz /tmp/svnlab/authz5 >/tmp/srv$p.log 2>&1 </dev/null &
  for i in $(seq 1 50); do ss -ltnp 2>/dev/null | grep -q ":$p " && break; sleep 0.1; done
done
rm -rf /tmp/lt1 /tmp/lt2
svn co -q http://127.0.0.1:18202/svn/lt /tmp/lt1 --username alice --password secret --non-interactive --no-auth-cache 2>/dev/null
svn co -q http://127.0.0.1:18203/svn/lt /tmp/lt2 --username alice --password secret --non-interactive --no-auth-cache 2>/dev/null
(cd /tmp/lt1 && N=$(date +%s%N) && echo e > e$N.txt && svn add -q e$N.txt && svn ci -q -m e --username alice --password secret --non-interactive --no-auth-cache >/dev/null 2>&1; echo $? >/tmp/e.rc)& p1=$!
(cd /tmp/lt2 && N=$(date +%s%N) && echo f > f$N.txt && svn add -q f$N.txt && svn ci -q -m f --username alice --password secret --non-interactive --no-auth-cache >/dev/null 2>&1; echo $? >/tmp/f.rc)& p2=$!
wait $p1 $p2
r1=$(cat /tmp/e.rc); r2=$(cat /tmp/f.rc)
[ "$r1" = 0 ] && [ "$r2" = 0 ] && echo "PASS two-process-disjoint rc=$r1/$r2" || echo "FAIL two-process-disjoint rc=$r1/$r2"
