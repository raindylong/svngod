# Segment 提交格式设计（存储引擎 v2）

> 目标：把“每修订 2–4 个对象 + 每修订一条 journal”改成“每批修订 1 个 segment 对象 + 一份小索引”，并配套在线 compaction、批量 WAL 与可增量迁移。本文是实现前的契约与落地计划；编解码/索引/键布局已先行实现并有单测。

## 1. 为什么需要

千万级修订压测的实测：

- 每仓 625k 修订约 **187 万个对象**（loose rev/revprops 125 万 + journal 62 万 + 分片包）；
- RustFS 上批量删除约 1 万键/秒，全量 GC/删除是**小时级**；
- 单次提交需要写 revblock、revprops、WAL 三个小对象，组提交也只能摊薄 CAS，摊不薄对象数；
- 对象数直接决定：存储成本、GC/备份时间、repair 列举时间、删除/生命周期操作成本。

Segment 格式把对象数从 O(revisions) 降到 O(revisions / batch)（再经 compaction 到 O(shards)），并让“一次写发布一批修订”成为原子操作。

## 2. 磁盘布局

```
segs/<shard>/<first>-<count>.seg   一个 segment（不可变）
segs/<shard>.idx                   该分片的 segment 索引（有序 JSON）
journal/<first>                    每批一条 WAL 记录（v2）
current                             HEAD（CAS 发布，语义不变）
texts/ chunks/ dirs/                大文件/分块/目录桶，保持原样
```

- `<shard>` 10 位十进制，`<first>/<count>` 12 位十进制；count 参与命名，compaction 发布更宽的 segment 时不会覆盖读者仍可能引用的旧对象。
- 一个 segment 不跨分片；跨分片的提交批在边界处拆成多个 segment。

### 2.1 Segment 编码

```
magic "SVNGSEG1" | u32 version=1 | u64 first | u64 count | u64 parent
for i in 0..count-1: u32 blockLen | u32 propsLen | block | props
u32 CRC32C(以上全部)
```

- block 为现有 `RevBlock.Marshal()` 字节，props 为 `RevProps.Marshal()` 字节；
- 解码校验 magic/长度/CRC/条目边界，任何不符即拒绝（视为损坏，交给副本/备份恢复）；
- 单 segment 目标大小建议 ≤ 4 MiB（约 1024–4096 个小修订），可通过 `SEGMENT_TARGET` 配置。

### 2.2 分片索引

`segs/<shard>.idx` 为有序 JSON：

```json
[{"first":1,"count":64,"key":"segs/0000000000/000000000001-000000000064.seg","sha256":"..."}]
```

- 条目按 first 升序，区间不重叠；
- 读路径二分查找 `rev ∈ [first, first+count)`；
- 索引本身是可变对象：提交/compaction 在**提交租约**内读最新版本后整体 Put；跨进程由租约串行化。

## 3. 提交协议（v2）

在现有“租约 → 构建 revblock/revprops → 发布”骨架上替换写阶段：

1. 取得提交租约，读 `current`（现有逻辑，含 OOD/base 检查）。
2. 为批内每个请求构建 `RevBlock` 与 `RevProps`（现有 `buildRevisionLocked`）。
3. 按分片边界把批拆成 1..n 组；每组：
   - `encodeSegment(first,count,parent)` → Put `segs/<shard>/<first>-<count>.seg`；记录 SHA-256。
4. 更新每组的分片索引：读取当前索引（租约内最新）→ 追加条目 → Put。
5. 为每组写一条 WAL：`journal/<first>`，`CommitRecord{Version:2, First, Count, Parent, Lease, SegmentKey, SegmentSHA, Created}`。
6. 一次 CAS `current` 到 `lastRev`（整批原子可见）。
7. 分片完成（`lastRev+1` 是分片边界）或条目数超过阈值时，执行 compaction（见 §5）。

崩溃窗口：

| 崩溃点 | 结果 |
|---|---|
| segment 写完、索引/WAL/current 未写 | 孤儿 segment，GC 可删；HEAD 不变，客户端未收到成功 |
| 索引写完、WAL/current 未写 | 索引指向未发布区间；读路径以 HEAD 截断，忽略越界条目；WAL 缺失则不前滚 |
| WAL 写完、current 未 CAS | 回放：`head+1 == First` 且 segment 存在/SHA 匹配 → 更新索引（幂等）→ CAS 到 `First+Count-1` |
| current CAS 成功 | 已发布；后续 compaction 不影响可见性 |

## 4. 读路径

`readBlock(rev)` / `loadRevProps(rev)` 在 v2 仓库（`SEGMENT_FROM <= rev`）走：

1. 计算 shard，载入索引（进程内缓存；未命中时**重新读取**一次以覆盖其他进程的新提交）；
2. 二分查找 segment 条目；
3. Get segment（按 key 缓存原始字节，超过缓存上限则只保留解码结果）；
4. 解码并在进程内缓存该 segment 的 block/props；
5. 返回 `rev-first` 对应的条目。

- 索引缓存失效：本地提交/compaction 后立即失效；远端提交造成的未命中触发一次 fresh 读取。
- segment 缺失（compaction 已删除旧对象）时：重新读取索引并重试一次；仍缺失则返回 `ErrNotFound`（触发副本读取/repair）。
- `verify`、`dump`、`blame`、`log` 都通过 `readBlock`/`loadRevProps`，无需单独改造。

## 5. 在线 compaction

触发：某分片的 segment 条目数 > 8，或该分片最后一次修订已发布（`lastRev+1 % shardSize == 0`）。

1. 在提交租约内（或独立 CAS 循环）读取分片索引；
2. 读出该分片全部 segment，按修订号拼接为一个大 segment（`first=shard*shardSize`, `count=跨度`）；
3. Put 新 segment（key 含 count，不覆盖旧键）→ Put 新索引（单条目）；
4. 延迟 60 秒（覆盖在途读者的旧索引解析窗口）后删除被合并的旧 segment；删除失败留给 GC 重试；
5. compaction 的写放大按“每分片一次”计，不随提交线性增长；可通过 `SEGMENT_TARGET` 控制目标大小。

读路径对 compaction 不阻塞：旧索引仍能解析到仍存在的旧 segment；新索引解析到新 segment。

## 6. 批量 WAL 与回放

- 每批一条记录（而不是每修订一条），键为 `journal/<first>`；
- 回放以 HEAD+1 为起点，连续读取；记录 v2 → 校验 segment 存在且 SHA 匹配 → 确保索引含该条目 → CAS 前滚；
- `TrimJournal` 在 v2 下按索引中的 segment 起点删除整批记录（精确键，不 list）；
- 保留窗口按“批数”而非修订数配置（默认 4096 批 ≈ 26 万修订 @K=64）。

## 7. 迁移（v1 → v2）

- `format` 增加 `SEGMENTS=1`、`SEGMENT_FROM=N`、`SEGMENT_TARGET=K`；N 之前仍按 v1 读（loose/pack），N 之后走 segment。这样支持**在线/分批**迁移。
- `svnadmin migrate --segments on <repo>`：在提交租约内逐个分片把已有 rev 读成 segment、写索引、推进 `SEGMENT_FROM`；每完成一个分片即可释放旧对象（或留给 GC）。
- 迁移可中断/续跑：已迁移分片以索引存在为准；未迁移分片继续按 v1 读。
- 回滚：保留 v1 对象直到全量迁移并验证通过；将 `SEGMENTS` 关闭即可回退（需停写）。

## 8. 兼容性与验证

- v1 仓库完全不受影响（`SEGMENTS` 默认 false）；新仓库可选择 `svnadmin create --segments`。
- 读/写/回放/compaction 全部有单测；集成验收：
  - 提交 100 万修订后对象数 ≈ revisions/K + shards（对比 v1 的 ≈3×revisions）；
  - `verify --all-revs`、`dump/load` round-trip、崩溃注入（WAL 前后）全部通过；
  - compaction 期间并发读不失败；旧索引读者在宽限期内可继续读；
  - 写延迟与 v1 对比、GC 耗时对比。

## 9. 落地顺序

1. **已完成**：format 字段、键布局（`segs/...`）、segment 编解码、分片索引编解码/查找（含 CRC/损坏/越界单测）。
2. 提交路径：`CommitBatch`/`CommitTxn` v2 分支、批 WAL、索引更新、CAS 与回放。
3. 读路径：`readBlock`/`loadRevProps`/fresh 变体接入 segment（含缓存与 fresh 重试）。
4. 在线 compaction 与旧 segment 延迟删除；GC/trim 适配。
5. `svnadmin create --segments`、`migrate --segments`、文档与基准。

## 10. 风险与取舍

- **读放大**：大 segment 需要整段读取（4 MiB 级）；用分片内小批 segment + 按需 compaction 控制，缓存命中后无额外成本。
- **索引争用**：分片索引每批重写；4096 修订/分片、K=64 时约 64 次写/分片，远小于每修订 1 次；租约已串行化。
- **compaction 写放大**：每分片合并一次，成本 O(分片大小)；可通过阈值/低峰调度。
- **缓存内存**：按 key 缓存解码后的 block/props，沿用现有 LRU 上限；大 segment 只缓存命中的修订块。
- **格式演进的兼容承诺**：v2 为 opt-in；v1 读取路径长期保留；`dump/load` 仍是跨版本迁移的兜底。