# 备份与恢复 SOP（Runbook）

本文是 svngod 的**可执行**备份/恢复规程：每一步都有命令、判定标准和失败处置。
规程中的门禁（`dump --verify`、`--expect-head`）是代码强制，不是约定，因此演练脚本
`scripts/drill-restore.sh` 可以当回归测试跑，结论可复现。

- 适用版本：含 `dump --verify` / `load --expect-head` / journal 哈希链（4.2）的 master
- 演练脚本：`scripts/drill-restore.sh [revisions] [workers]`
- 相关文档：`监控与告警.md`（verify 指标与告警）、`配置与多实例部署.md`（mirror/geo/分片）

---

## 1. 为什么备份必须带门禁

svngod 的历史是**不可变对象 + WAL + 仲裁**的组合。这意味着：

- 单个对象损坏时，读路径可能仍然"能读出东西"（例如 text 走 delta 重建、rev-block 只读到部分字段），
  于是**一份能 dump 出字节的备份未必是一份能恢复的备份**。
- 更糟的是**篡改**：journal 记录被改写后，记录本身格式合法、CRC 正确，只有 WORM 哈希链能发现。

所以备份动作必须满足三条：

| 要求 | 实现 | 违反后果 |
|---|---|---|
| 备份源本身可信 | `dump --verify` 先跑链校验 + HEAD 可读性校验 | 备份里固化了一份坏数据，恢复时才发现 |
| 备份副本可认证 | dump 打印 `head/bytes/sha256` 清单 | 传输截断/被替换无法察觉 |
| 恢复结果可断言 | `load --expect-head N` | 恢复"成功"但少了几百个修订 |

`dump --verify` 在发现损坏时**直接退出非零且不产出文件**（写 `<output>.partial`，
成功后原子 rename），所以不会在可信路径上留下截断文件。

---

## 2. 备份（每日）

### 2.1 单仓备份

```bash
#!/usr/bin/env bash
set -euo pipefail
REPO=myrepo
DEST=/backup/svngod/$REPO
STAMP=$(date -u +%Y%m%dT%H%M%SZ)

mkdir -p "$DEST"
# --verify: 链 + HEAD 门禁；损坏则非零退出，不产生文件
svnadmin dump \
  --endpoint 10.0.0.10:9000 --access-key "$AK" --secret-key "$SK" \
  --bucket svn --prefix repos \
  --verify --output "$DEST/$REPO-$STAMP.dump" "$REPO" \
  2> "$DEST/$REPO-$STAMP.manifest"

# 用清单里的 sha256 认证这份副本
WANT=$(sed -n 's/.*sha256=\([0-9a-f]\{64\}\).*/\1/p' "$DEST/$REPO-$STAMP.manifest")
GOT=$(sha256sum "$DEST/$REPO-$STAMP.dump" | awk '{print $1}')
[ "$WANT" = "$GOT" ] || { echo "BACKUP CORRUPT: $REPO-$STAMP"; exit 1; }
```

清单内容形如（写 stderr，因此重定向 stdout 不会污染数据流）：

```
dump: repo=myrepo head=1048576 bytes=734003200 sha256=9f2c...c1
```

**清单必须与 dump 一起存档。** 没有 head 值，恢复时就无法断言完整性。

### 2.2 多仓批量

```bash
# 仓库清单来自管理 API（见《管理API与热加载.md》）
for r in $(curl -fsS http://127.0.0.1:18080/_api/v1/repos | jq -r '.[].name'); do
  ./backup-one.sh "$r" || echo "BACKUP FAILED: $r" >> /var/log/svngod/backup-errors.log
done
```

用本地 `--repo-root` 部署时，直接遍历目录名即可：

```bash
for r in $(ls /srv/svngod/repos); do
  svnadmin dump --verify --output "/backup/$r-$(date -u +%Y%m%dT%H%M%SZ).dump" "/srv/svngod/repos/$r" \
    || echo "BACKUP FAILED: $r" >> /var/log/svngod/backup-errors.log
done
```

- 单仓失败**不阻断**其它仓（否则一个大仓会拖垮整轮备份）。
- 但失败必须被采集：`backup-errors.log` 非空即告警，且**当天不得清除上一份可用备份**
  （保留策略：至少 2 份成功备份才允许删除更早的）。

### 2.3 保留策略

| 周期 | 保留 |
|---|---|
| 日备 | 14 天 |
| 周备（周日的日备另存） | 8 周 |
| 月备（每月 1 日的日备另存） | 12 月 |

删除前校验：待删备份之后**必须存在至少 1 份成功备份**，且其 `head` ≥ 待删备份的 `head`。
（`head` 倒退说明中间发生过误恢复，见 §5。）

---

## 3. 恢复（演练或真实）

### 3.1 恢复到新仓（推荐，先在旁路验证）

```bash
DEST=/restore/myrepo
svnadmin load \
  --endpoint 10.0.0.10:9000 --access-key "$AK" --secret-key "$SK" \
  --bucket svn --prefix restore \
  --input /backup/svngod/myrepo/myrepo-20261112T030000Z.dump \
  --expect-head 1048576 \
  myrepo
```

`--expect-head` 与清单 `head=` 必须一致。不一致时命令失败并给出实际 head：
**这是保护，不是麻烦**——它意味着 dump 不完整或来自别的仓库。

### 3.2 恢复后必须跑的三项断言

```bash
# ① 全历史 + 每个 text（含 delta 链）+ HEAD 树
svnadmin verify --endpoint ... --all-revs --texts --workers 8 myrepo

# ② WAL 哈希链（防篡改证据）
svnadmin verify --endpoint ... myrepo | grep 'journal hash chain'
# 期望：verify: journal hash chain intact (revs 1..1048576)

# ③ 独立再 dump，与备份逐字节比对（最强的一致性证明）
svnadmin dump --endpoint restore ... --output /tmp/redo.dump myrepo
sha256sum /tmp/redo.dump   # 必须等于清单里的 sha256
```

第 ③ 项值得强调：它同时覆盖 revprops、节点树、文本内容、变更方向（add/modify/delete/copy），
任何一项偏移都会让字节流不同。**这是恢复演练的最终判据。**

### 3.3 演练（不改生产）

演练必须与生产用同一条代码路径（WAL + 仲裁），否则"链校验通过"没有意义：
**纯 FileStore 没有 journal，链校验是 no-op**。因此演练用双目录 WAL mirror：

```bash
# 双目录 WAL mirror（最快，约 5 秒）
scripts/drill-restore.sh 200 8

# 真实 S3 mirror（验收环境，40 修订约 2 分钟）
DRILL_MIRROR_FILE=/etc/svngod/mirror.json scripts/drill-restore.sh 40 8
```

脚本依次执行：建仓 → 提交 → `dump --verify` → sha256/字节数认证 → `load --expect-head`
→ UUID 一致性 → `verify --all-revs --texts` → 再 dump 比对 → 负向控制。任一步失败即非零退出。

**两种模式的分工**：

| | 本地目录 | `DRILL_MIRROR_FILE`（真实 S3） |
|---|---|---|
| 正向链路 | ✅ | ✅ 备份/恢复/校验/幂等全跑 |
| 对象损坏负向控制 | ✅（直接改文件） | 跳过（并打印原因） |
| 每次运行隔离 | 临时目录 | 独立 bucket 前缀 + 独立仓名 |

负向控制依赖直接改写磁盘对象，因此只在本地模式下执行；**门禁逻辑在 svnadmin 里，
与后端无关**，所以本地覆盖是有效的。脚本会显式打印跳过，不会静默通过。

`--mirror-file` 同时接受 `access`/`secret` 与配置里的 `access-key`/`secret-key`，
所以 `storage.mirrors` 段可以原样复制使用；S3 成员缺少凭据时会直接报
`missing credentials`，而不是发出匿名请求后显示 Access Denied。

**真实 S3 上的耗时预期**：种子提交约 0.5 rev/s（Put 往返 95–200 ms + WAL 放大，
详见《性能基线与回归.md》），其余步骤很快。因此演练规模按分钟预算选，默认 40 修订。

**负向控制是演练的核心**——只证明"好的能恢复"不够，还必须证明"坏的能被拦住"：

| 注入 | 期望 |
|---|---|
| journal 记录被改写 | `dump --verify` 非零退出，不产出文件 |
| journal 中间记录缺失 | 报 `chain gap at rev N`，非零退出 |
| HEAD rev-block 被覆写 | 报 integrity issue(s) at HEAD，非零退出 |
| `--expect-head` 给错值 | `load` 失败并说明实际 head |

建议节奏：**每次发版跑一遍**（脚本约 1 分钟），**每季度做一次真实数据演练**
（生产 dump 恢复到旁路 region，跑 §3.2 全部三项）。

---

## 4. 场景处置

### 4.1 备份失败：`dump --verify` 报链断裂

```
svnadmin: dump: refusing to back up: journal: hash chain broken at rev 812345: predecessor was rewritten or forged
```

链断裂意味着 **rev 812344 的 WAL 记录与其后继不匹配**。两种可能：

1. **真篡改**：有人改写了历史。→ 立即保全证据：`svnadmin journal -n 50` 导出附近记录，
   冻结该仓写入，走安全事件流程；同时从**最近的完好备份**恢复（备份的 head 必须 < 812344）。
2. **存储层损坏**（对象被替换/部分写）。→ 用 `svnadmin replicas -verify -counts --mirror-file`
   定位哪个副本坏了，`-repair-mirror` 从健康副本修复；修复后重新跑 `dump --verify`。

不要用"跳过 --verify 再 dump"来绕过——那会把损坏固化进备份链。

### 4.2 备份失败：HEAD 不可读

```
svnadmin: dump: refusing to back up: 2 integrity issue(s) at HEAD 812345
dump: corrupt rev=812345 path="/trunk/x.c": ...
```

先判断 `rebuildable`：

- `rebuildable=true`（delta base 完好，只是 delta 坏了）→ `svnadmin gc` 后可重建，再试。
- `rebuildable=false` → 数据本体缺失。从副本恢复：`svnadmin replicas -repair ...`；
  副本也坏则回退到最近备份，接受 §5 的 head 倒退流程。

**绝不能**用当前 HEAD 的一份"能读出部分内容"的 dump 覆盖上一份完好备份。

### 4.3 恢复后 head 与清单不符

`--expect-head` 已经拦住了这种情况，但如果手工恢复（不带该参数）后发现：

```bash
svnadmin info ... myrepo | grep head   # 少于清单 head
```

说明 dump 被截断。**不要在该仓上继续提交**——先重新取回完整 dump 重做恢复。
若生产仍在服务，跳过本仓、继续其它仓的恢复，把本仓单列处理。

### 4.4 恢复后的仓库需要继续服役

1. `verify --all-revs --texts` 必须干净（§3.2 ①②③ 全过）。
2. 恢复仓的 `uuid` 与源仓相同（dump/load 保留 uuid）——**不要**让两个相同 uuid 的仓同时对外服务，
   客户端会认为它们是同一个仓而产生混乱。切换时应停旧仓或改客户端 URL。
3. 切流前确认 `svngod_repo_head{repo}` 单调（见 `监控与告警.md`），避免 head 倒退告警误报。

---

## 5. 演练记录模板

每次演练留档，便于审计与趋势比对：

```
演练日期：2026-11-12
代码版本：88f901c
场景：完整恢复（revs=1048576，mirror quorum=2）
备份：bytes=734003200 sha256=9f2c...c1 head=1048576
恢复耗时：dump 41s / load 63s / verify(all-revs,texts) 12m18s
断言：①verify 干净 ②chain intact ③re-dump 字节一致
负向控制：4/4 全部按预期拦截
结论：PASS
异常/改进项：load 阶段 CPU 未打满，可评估 --workers
```

---

## 6. 与其它机制的分工

| 机制 | 覆盖 | 不覆盖 |
|---|---|---|
| WAL + 仲裁（quorum） | 单节点崩溃、瞬时故障 | 逻辑损坏、误删仓、跨 region 灾难 |
| mirror repair / replicas | 副本间漂移与对象损坏 | 全副本同时坏 |
| 后台 scrub（`--scrub-interval`） | 静默损坏的**早期发现** | 恢复手段本身 |
| WORM 哈希链 | 历史被改写（审计证据） | 尾部记录被删（靠 `repo_head` 单调性告警） |
| **本文档 dump/load** | 误删、灾难、跨环境迁移、合规归档 | 分钟级 RPO（dump 是快照语义） |

**大批量恢复**：`load` 已接入批量提交（默认每批 1000 修订，`--batch N` 可调），
一批只付一次 commit lease + WAL + HEAD CAS。真实 S3 上 1000 修订从 34.9 分钟
（`--batch 1`）降到约 13 秒；10000 修订用默认配置为 **43 秒**（233 rev/s，峰值内存 69 MB），
结果与逐条导入**字节一致**（详见《性能基线与回归.md》）。恢复目标仓建议建为 segment 格式
（`svnadmin create --segments`）。

**备份窗口要按 dump 而不是 load 估算**：10000 修订的 dump 耗时 340 s，比 load 慢一个
数量级（dump 是串行读并重建每修订的变更集）。百万级未实测，部署前须在目标数据量标定。

RPO/RTO 参考（实测于两节点 mirror）：日备 RPO ≤ 24h。
RTO 由备份窗口（dump）与恢复窗口（load + verify）构成，前者是瓶颈：
10000 修订 dump 340 s、load 43 s、verify <1 s。百万修订的 RTO **未在此环境实测**，
不要按千修订数字线性外推，应在目标数据量上标定后再承诺。
需要更小 RPO 时用 geo 复制（`type=geo`）做热备，dump 仍作为**离线归档与
最后手段**保留。