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

### 2.1 Segment 编码（v2，带条目表）

```
magic "SVNGSEG1" | u32 version=2 | u64 first | u64 count | u64 parent
count × [ u64 blockOff | u32 blockLen | u32 propsLen | u32 CRC32C(entry) ]
for i in 0..count-1: u32 blockLen | u32 propsLen | block | props
u32 CRC32C(以上全部)
```

- block 为现有 `RevBlock.Marshal()` 字节，props 为 `RevProps.Marshal()` 字节；
- 条目表放在**头部之后**，因此读到 count（索引里有）就能用一次小的 Range GET 取回 `36 + 20×count` 字节的表；
- 每个条目独立 CRC32C，支持只取一个条目并校验；整对象的 CRC32C（尾部）仍用于全量读；
- v1 段（无表）保持可读：版本不是 2 时回退到整对象读，不需要迁移；
- 解码校验 magic/长度/偏移单调/CRC/条目边界，任何不符即拒绝（视为损坏，交给副本/备份恢复）；
- 单 segment 目标大小建议 ≤ 4 MiB（约 1024–4096 个小修订），可通过 `SEGMENT_TARGET` 配置。

### 2.2 按修订 Range 读

读一条修订只需要“表 + 该条目”，不是整个 segment：

1. `Get(key, Range{0, 36+20×count})` → 解析表（进程内 LRU 缓存 256 个分片段，表命中即省掉这次 GET）；
2. `Get(key, Range{entryOff+8, blockLen+propsLen})` → 校验条目 CRC 后返回 block/props 的拷贝。

顺序访问自动升级为整段读：同一 segment 内连续请求 `rev, rev+1` 时，第二次改为一次整对象 GET，解码后放进进程内“已解码段”缓存（2 段），后续修订零网络开销——`svn log`/`blame`/`dump` 因此仍是一次 GET 服务一整个段，而 checkout 这类跳读只付 KB 级流量。

`svnadmin verify` 会临时打开全量读模式（`fullReads`），确保每个段对象都按索引里的 SHA-256 校验；Range 读只能校验单条目 CRC，不承担这个职责。

`svnadmin verify` 在遍历修订前先校验**分片索引的覆盖性**：每个分片必须从 `SEGMENT_FROM`（或分片起点）开始、连续覆盖到 `HEAD`（末分片只到 HEAD）。缺条目会让一段修订永久不可读，而 HEAD 和其它对象看起来都正常，因此单独报错。`--workers N` 按修订区间并行校验，结果按修订号排序合并；分片索引检查始终在并行段之前完成。200,000 修订实测：单线程 3.9s，8 线程 3.5s（瓶颈已是 CPU 解码，而非 S3 往返）。

升级语义：只有新写入（含 compaction 重写）的段是 v2。历史 v1 段保持可读、走整对象读，被 compaction 合并时自动升级；不需要停机迁移，也不需要改写已有对象（段键由 first/count 决定，原地改写会让并发读者读到校验不符的字节）。

S3（RustFS）实测 50 修订、19,408 字节的段：随机读一条修订传输 1,423 字节 / 2 次 GET；连续读 10 条传输 20,837 字节 / 3 次 GET（表+首条目，随后整段一次）。

### 2.3 分片索引

`segs/<shard>.idx` 为有序 JSON：

```json
[{"first":1,"count":64,"sha256":"..."}]
```

- 条目只存 first/count/sha256，对象键由仓库前缀 + first/count 推导（scoped store 不会二次加前缀）；
- 条目按 first 升序，区间不重叠；
- 读路径二分查找 `rev ∈ [first, first+count)`；
- 索引本身是可变对象：提交/compaction 在**提交租约**内读取，写入用 CAS（见 §3.1）；租约负责常规互斥，CAS 负责租约过期/停顿瞬间的安全底线。

### 3.1 索引的原子写（CAS）

租约正常时只有写者在动索引；但进程停顿、租约 TTL 过期后可能有第二个写者。为避免“后写覆盖前写、HEAD 指向索引里已不存在的条目”，索引写入遵守：

- **读**：一次 GET 取回索引内容，用 `ContentETag(内容)` 作为期望 ETag（FileStore/MemoryStore/单段 S3 对象的 ETag 就是内容 MD5；遇到不透明 ETag 的网关，`Store.CAS` 会自动回退为读回内容比对）。因此常规路径不额外增加往返。
- **写**：对象存在则 CAS，不存在则 `CreateIfAbsent`；失败返回 `ErrSegmentIndexConflict`，重试时重新读取并合并。
- **拒绝覆盖已发布区间**：要写的区间与现有条目重叠、且现有条目起点 ≤ HEAD（即这些修订已经发布）时，直接报冲突，绝不覆盖；提交失败、客户端重试。
- **回收未发布条目**：重叠条目的起点 > HEAD 说明它属于一次没有完成的发布（索引写了、HEAD CAS 未执行，或进程崩溃）。此时从 segment 对象重建该分片（重建会丢弃 > HEAD 的条目），再重试，避免这些修订号被永久占住。
- **连续性校验**：写回前检查“分片从起点到本批末修订连续、无空洞/重叠”；会写出不连续索引的写入被拒绝并走重建，宁可失败也不写坏索引。
- **失败回滚**：索引写成功后，若 WAL 写失败或 HEAD CAS 失败，撤回本次写入的条目（按 first+sha 精确匹配），使重试不会被未发布条目挡住。
- **发布后自检**：HEAD CAS 成功后重读一次索引，确认本批条目仍在；若被租约违规者覆盖，立即补写并打印告警。这是“HEAD 说已发布、索引却不认”的最后一道兜底。


## 3. 提交协议（v2）

在现有“租约 → 构建 revblock/revprops → 发布”骨架上替换写阶段：

1. 取得提交租约，读 `current`（现有逻辑，含 OOD/base 检查）。
2. 为批内每个请求构建 `RevBlock` 与 `RevProps`（现有 `buildRevisionLocked`）。
3. 按分片边界把批拆成 1..n 组；每组：
   - `encodeSegment(first,count,parent)` → Put `segs/<shard>/<first>-<count>.seg`；记录 SHA-256。
4. 更新每组的分片索引：读取当前索引（租约内最新）→ 追加条目 → **CAS 写回**（规则见 §3.1）。
5. 为**整批写一条** WAL：`journal/<批首修订>`，`CommitRecord{Version:2, First, Count, Parent, Lease, Created, Groups:[{First, Count, SegmentKey, SegmentSHA}...]}`。一条记录描述全部组分片段，回放因此是“全有或全无”——按组各写一条时，崩溃在两条记录之间会让回放发布半个批次（2026-11 故障注入发现并修复）。
6. 一次 CAS `current` 到 `lastRev`（整批原子可见）。
7. 分片完成（`lastRev+1` 是分片边界）或条目数超过阈值时，执行 compaction（见 §5）。

崩溃窗口：

| 崩溃点 | 结果 |
|---|---|
| segment 写完、索引/WAL/current 未写 | 孤儿 segment，GC 可删；HEAD 不变，客户端未收到成功 |
| 段/索引/覆盖层写入未到备副本 | 镜像 repair 会列出 `segs/` 与 `seg-revprops/` 并补齐（原先遗漏，2026-11 修复）；`svnadmin replicas` 可发现漂移 |
| 索引写完、WAL/current 未写 | 条目在 HEAD 之上，属未发布；下次同名区间提交会重建索引并回收它 |
| 索引/WAL 写完、current CAS 失败 | 回滚索引条目；HEAD 不变，客户端重试 |
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

触发：某分片最后一次修订已发布（`lastRev+1 % shardSize == 0`），提交线程把分片号放入后台队列，由仓库级 worker 异步合并。

1. worker 先确认 HEAD 已越过分片末修订，再获取**提交租约**（与提交串行，避免合并覆盖刚发布的批）；
2. 在租约内读取分片索引；
3. 读出该分片全部 segment，按修订号拼接为一个大 segment（`first=shard*shardSize`, `count=跨度`）；
4. Put 新 segment（key 含 count，不覆盖旧键）→ 并发 Put 新索引（单条目）与批 WAL 记录（两者都必须先于 HEAD CAS 落盘，但彼此无序）；
5. 延迟 60 秒（覆盖在途读者的旧索引解析窗口）后删除被合并的旧 segment；删除失败留给 GC 重试；
6. compaction 的写放大按“每分片一次”计，不随提交线性增长；**合并结果按 `SEGMENT_TARGET` 切段**（2026-11 前只读未生效）：目标 64 时一个 4096 修订分片会得到 64 个段而不是一个 4 MiB 段，单个大于目标的批次保持原样；已符合目标的段不重写、重复执行幂等。
7. 合并时的源段读取并行（8 路），大分片的 gc 不再逐段串行往返。

异步化的原因与边界：

- 合并一个满分片是 O(分片大小)，原先由“关闭该分片的那个提交”同步执行：提交延迟出现尖峰，且客户端超时取消请求会把合并打断在写索引之前（安全，但白做）。
- worker 每次合并都重新确认 `HEAD >= 分片末修订`；分片一旦完成就不会再接收新修订，索引在租约内是稳定的。
- 租约被占用时按 1s→2s→… 退避重试，最多 6 次；仍失败只记日志丢弃——compaction 只影响读放大，不影响正确性，下一次分片完成或 GC 会再做。
- 队列（256）满时直接丢弃该分片号，语义同上。
- 进程在合并中途退出：旧索引未被替换，旧 segment 全部有效；已写出的合并段成为孤儿对象，由 GC 回收。

读路径对 compaction 不阻塞：旧索引仍能解析到仍存在的旧 segment；新索引解析到新 segment。

### 5.1 运维合并（`svnadmin gc`）

后台 worker 只在分片**填满**时触发；仓库停在半个分片、或迁移按分片写入时，索引会累积很多小段。`svnadmin gc` 对 v2 仓库额外做两件事：

- `CompactSegments`：对每个 index 条目 > 1 的分片逐段合并（含未填满的当前分片），每个分片单独获取提交租约；合并前后都重读索引，缺洞（`span ≠ Σcount`）的分片拒绝合并并报错，避免把空洞拼进段里。
- 删除只作用于**主副本**（`DeletePrimary`，与 loose 清理一致）：备副本上的残留段无人引用、无害，而在 quorum 模式下镜像成千上万条 DELETE 会耗尽备节点写预算并把它踢下线（实测）。
- `CollectOrphanSegments`：列出 `segs/` 下所有 `.seg`，与所有分片索引对照，删除“无索引引用且对象年龄 ≥ 1 分钟”的对象——即中途死掉的 compaction 留下的合并段、以及 CLI compaction 退出后遗留的旧段。**删除阶段持有提交租约**：compaction 是“先写合并段、再换索引”，租约保证删除时没有活跃 compaction 处于这两步之间；索引集合在租约内重新读取，因此不存在“列完清单后又有提交引用它”的窗口。租约拿不到（有提交在跑）时本次跳过清扫，不影响正确性。
- 幂等：重复执行没有副作用；被误删的分片索引条目会在下次 `verify` 暴露（段 SHA 校验失败），由 WAL 回放补齐。
- 键形式：scoped store（`svnadmin --endpoint`）的仓库前缀为空，键构造会带前导斜杠而列表返回相对键。GC 的“引用集合 vs 列表结果”比较必须先归一化（`Keys.ListingPrefix`），否则会把所有活对象判成无引用——这是 2026-11 修复的一个真实数据安全缺陷，回归测试用 scoped store 桩覆盖 text/chunk/segment 三条清扫路径。
- `gc -texts` 的历史引用扫描按修订区间 8 路并行（只读不可变块）；20 万修订实测“扫描+compaction+journal 裁剪”合计 9 秒（此前串行扫描约 141 秒）。
- 默认 `svnadmin gc` 只做上述结构性回收（segv2 20 万修订实测 0.45 秒）；文本引用扫描要遍历全部修订（同类仓库约 141 秒），改为显式 `-texts`——它只回收孤立文本对象，跳过不会影响正确性。

### 5.2 索引丢失与自愈（`repair --rebuild-index`）

分片索引是**可变对象**（提交/compaction 在租约内重写它），而 segment 是**不可变对象**且先于索引落盘。因此索引丢失、少一条、或后端吞掉一次 PUT 时，segment 对象仍是完整的事实源，索引可以从对象重建：

- 读路径发现“`rev ≤ HEAD` 但索引里没有”时，对该分片做一次**限频**（30 秒）重建，然后重试；读请求因此自愈，不再需要人工介入；
- `svnadmin repair --rebuild-index <repo>` 对 0..HEAD 的所有分片做一次全量重建，输出重建数量，幂等；
- 重建在**提交租约**内进行，列出 `segs/<shard>/`、逐个读对象重算 SHA-256，最后整体 Put 索引，因此不会吞掉并发提交刚追加的条目；
- **重叠策略**：compaction 合并后，旧的窄 segment 可能残留（延迟删除 + CLI 进程退出导致未执行）。同一批修订在各副本里的 block/props 是相同的不可变内容，所以保留“覆盖范围最宽”的对象、丢弃被完全包含的对象；**部分重叠**（本格式不可能产生）视为损坏并拒绝写入，不猜；
- 拒绝把有洞的结果写进索引（对象缺失时直接报错），重建失败只记日志，不影响读路径返回原始错误。
- `svnadmin verify --all-revs --repair-index` 在遍历前先重建索引；`svnadmin repair --rebuild-index` 同时清理“覆盖列表里有、对象已丢失”的死条目（恢复为已提交值），并在输出中报告数量。

### 5.3 修订属性覆盖层（segment 仓库的 revprop 编辑）

segment 不可变，但修订属性可变（`svn propset --revprop`）。v2 仓库把属性编辑存成覆盖对象，而不是重写 segment：

```
seg-revprops/<shard>/<rev>   该修订的完整生效属性集
seg-revprops/<shard>.idx     该分片有覆盖的修订号列表（有序 JSON）
```

- **读**：先看分片覆盖列表（进程内缓存 1 秒，与 revprops 缓存同 TTL）；未命中则直接读 segment 里的提交值，**常规修订不增加任何对象请求**；命中则读覆盖对象。`GetRevPropsFresh` 绕过列表缓存，始终查一次覆盖对象，跨进程编辑立即可见。
- **写**：先 CAS（不存在则 `CreateIfAbsent`）写覆盖对象，再 CAS 把修订号加入分片列表。两个进程并发改同一修订的不同属性时，CAS 竞争失败的一方重读、合并、重试，不会丢更新；先写对象后写列表，崩溃只会留下不可见的覆盖对象，不会出现“列表里有、对象没有”。
- 覆盖对象是完整的生效属性集，所以读取只请求一次；列表上限是分片修订数，属于防损坏的合理性检查。
- **列表说“有覆盖”、对象却不存在时报错，不静默回退到段里的提交值**——回退等于把一次属性编辑悄悄还原；`verify` 会同时报告该修订，便于从副本恢复。
- 未被覆盖的修订走 segment；被覆盖的修订以后的读取不再读 segment 里的属性（block 仍在 segment 中）。

## 6. 批量 WAL 与回放

- 每批一条记录（而不是每修订一条），键为 `journal/<批首修订>`；记录里 `Groups` 列出该批写出的每个分片段；
- 回放以 HEAD+1 为起点，连续读取；记录 v2 → 先校验**所有组**：区间连续、每个 segment 存在且 SHA 匹配；全部通过后才补齐各分片索引并一次 CAS 到批末，任一缺失则整批不发布；
- 回放的推进依据是记录自带的 `Count`，每步重新读取 HEAD，因此**批次大小不受窗口常数限制**（实测 300 修订的单批在 HEAD CAS 崩溃后完整前滚 2..301）；`CommitBatch` 只保留 65536 条的合理性上限；
- 旧格式（无 `Groups` 的单组记录）按单组处理，升级不需要迁移；
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
  - 提交 100 万修订后对象数 ≈ revisions/K + shards（对比 v1 的 ≈3×revisions）；实测 20 万修订 = 525 对象（§11.1）；
  - `verify --all-revs`、`dump/load` round-trip、崩溃注入（WAL 前后）全部通过；
  - compaction 期间并发读不失败；旧索引读者在宽限期内可继续读；
  - 写延迟与 v1 对比、GC 耗时对比。

## 9. 落地顺序

1. **已完成**：format 字段、键布局（`segs/...`）、segment 编解码、分片索引编解码/查找（含 CRC/损坏/越界单测）。
2. **已完成**：提交路径 v2（`CommitBatch` 分支、按分片拆段、批 WAL、索引更新、汇总 CAS、CAS 竞争的逐段认领）；`CommitTxn` 在 v2 下走批量入口。
3. **已完成**：读路径（`readBlock`/`loadRevProps`/fresh、索引缓存未命中 fresh 重试、segment SHA 校验）。
4. **已完成**：分片完成时在线 compaction（新键含 count、旧段 60 秒后删除）；v2 的 journal 裁剪按索引精确键删除；`CollectLoose` 在 v2 下跳过（无 loose）。
5. **已完成**：`svnadmin create --segments [--segment-target N]`；`svnadmin migrate --segments on [--purge] <repo>` 离线/增量迁移（按分片写段、HEAD 变动即中止、全部成功后才切格式、可选 purge 旧对象）；基准见 §11。

端到端验证（2026-11-09）：v2 仓库用原生 svn 完成 12 次提交 + 4 并发 ×3 提交（产生 count=3/4 的批段），`svn log/cat/co`、`verify --all-revs`、`dump/load` round-trip、重启后读取全部通过；磁盘上仅 rev 0 为 loose，其余在 `segs/`。

## 11. 实测（2026-11-09，本地 file 后端，20,000 修订，K=64）

| 指标 | v1（每修订对象） | v2（segment） |
|---|---|---|
| 写入 20k 修订 | 26.0s（768/s） | **2.0s（9,796/s）** |
| 仓库文件数 | 7,254 | **332** |
| 迁移 v1→v2（含 purge） | — | 4.45s，迁移后 14 个文件 |
| 迁移后继续提交 | — | 1,000 修订 0.1s（8,320/s） |
| 全量 verify | — | 20k/21k 修订 `no integrity issues` |

- 迁移过程中发现并修复：`segmentEntry` 返回段内子切片会让每个缓存的 revblock 持有整个多 MB 段，20k 修订的 `verify` 因此 OOM；现返回拷贝。
- 读取路径对段内修订取的是拷贝，缓存按 block/segment 分别计量。

### 11.1 实测（S3 单 RustFS `svn-ext/svn-ext`，200,000 修订，K=64）

| 指标 | v1（每修订对象 / 组提交） | v2（segment） |
|---|---|---|
| 写入吞吐 | 约 33–48 次/秒 | **385.2 次/秒**（200,000 修订 / 519s） |
| 失败数 | — | 0 |
| 仓库对象数 | 40 万级 | **525**（`segs/` 520 + 控制对象 5） |
| 仓库体积 | — | 133 MiB |
| 全量 verify | — | `1..200000 no integrity issues` |

- S3 上每条 PUT 约 5–25ms，v1 每修订 2 次以上 PUT（block/props + journal/HEAD）成为吞吐上限；v2 每 64 修订只写 1 段 + 1 索引 + 1 批 WAL，再叠加一次 HEAD CAS，对象数下降 3 个数量级。
- **过程中发现并修复（关键）**：`segmentRef`/`CommitRecord` 早期保存的是绝对对象键，在 scoped store（`svnadmin --endpoint`，内部 `RepoPrefix=<prefix>` + `BaseRepo=<repo>`）下该键会被二次加前缀，读回 404；现索引与 WAL 只记录 `First/Count/SHA256`，读取时用 `r.keys.Segment(r.shard(first), first, count)` 派生，与运行期前缀完全一致。修复后重新灌入 200k 修订并通过全量 verify。
- 相对键同时消除了「同一仓库在不同前缀/不同实例下迁移后可读」的隐患：键由仓库内位置推导，不含部署路径。

### 11.2 写故障注入（原子性回归）

`internal/fs/atomicity_test.go` 在提交发布路径的每一个写点注入失败，分两种模型：

- **soft**：该次写失败，进程继续（回滚路径可执行）；
- **hard**：失败后所有后续写都失败（模拟进程死亡，回滚也执行不了）。

覆盖单分片（2 修订）与跨分片（10 修订，2 组）两种批次。每次故障后重新打开仓库（触发 WAL 回放），断言：HEAD 只能是旧值或“旧值+整批长度”（绝不出现部分发布）；HEAD 之下全部可读且 `verify` 干净；随后一次正常提交恰好落在 HEAD+1（证明没有残留条目阻塞）。

## 10. 风险与取舍

- **读放大**：大 segment 需要整段读取（4 MiB 级）；用分片内小批 segment + 按需 compaction 控制，缓存命中后无额外成本。
- **索引争用**：分片索引每批重写；4096 修订/分片、K=64 时约 64 次写/分片，远小于每修订 1 次；租约已串行化。
- **compaction 写放大**：每分片合并一次，成本 O(分片大小)；可通过阈值/低峰调度。
- **缓存内存**：按 key 缓存解码后的 block/props，沿用现有 LRU 上限；大 segment 只缓存命中的修订块。
- **格式演进的兼容承诺**：v2 为 opt-in；v1 读取路径长期保留；`dump/load` 仍是跨版本迁移的兜底。