# svngod — S3 存储引擎实现细则（实现级设计）

> 配套文档 | 归属：《Go-SVN服务器架构设计方案》第 5 章 | 版本 v1.3 | 2026-11-02（含 WAL/法定人数与 RustFS 实测）
>
> 目标：把第 5 章的"分片 + 封装 + 不可变对象 + 原子指针"落地为**可直接照此编码**的实现级设计：字节级对象格式、写/读路径、增量策略、svndiff 编解码、缓存、封装任务、GC、Go 类型与伪代码。

---

## 1. 范围与目标

本细则覆盖单仓库存储引擎的完整实现设计，交付：

1. **存储抽象层**（`Store`）与 S3 适配（含条件写/CAS 的跨提供商实现）。
2. **数据模型与字节级序列化**：node-rev、rev_block、分片 idx、revprops、current。
3. **对象布局与生命周期**、打包标记。
4. **写路径**（事务 → 提交 → CAS → 崩溃恢复）。
5. **读路径**（shard 解析、node-rev 定位、文本重构）。
6. **增量策略与 deltification**（提交时全量化决策，适配不可变对象）。
7. **svndiff 编解码**（字节级窗口格式）。
8. **缓存层**、**分片封装任务**、**垃圾回收**、**校验**。
9. **Go 类型与关键伪代码**、实现要点与风险。

**关键不变量**：数据对象先于 `current` 指针可见；任何已提交修订的内容不可变；崩溃不产生半提交，只产生可被 GC 回收的孤儿对象。

---

## 2. 存储抽象层（Store）与 S3 适配

### 2.1 Store 接口

```go
type Range struct{ Offset, Length int64 }

type Store interface {
    Put(ctx context.Context, key string, data []byte) error
    PutFrom(ctx context.Context, key string, r io.Reader, size int64) error // 流式大对象
    Get(ctx context.Context, key string, rng *Range) ([]byte, error)
    GetTo(ctx context.Context, key string, w io.Writer) (int64, error)     // 流式读取
    Exists(ctx context.Context, key string) (bool, error)                   // 只判断存在，不读 body
    Head(ctx context.Context, key string) (etag string, exists bool, err error)
    CreateIfAbsent(ctx context.Context, key string, data []byte) error
    CAS(ctx context.Context, key string, expectETag string, data []byte) (newETag string, err error)
    ListPrefix(ctx context.Context, prefix, continuation string, max int) ([]ListedObj, string, error)
    Delete(ctx context.Context, keys []string) error
}

// 可选能力：法定人数寄存器（提交租约）。MirrorStore 实现。
type QuorumRegister interface {
    CreateIfAbsentQuorum(ctx context.Context, key string, data []byte, quorum int) error
    CASQuorum(ctx context.Context, key, expect string, data []byte, quorum int) error
    DeleteIfMatchQuorum(ctx context.Context, key, etag string, quorum int) error
    QuorumGet(ctx context.Context, key string) (data []byte, copies int, err error)
}

// 可选能力：WAL 与副本数量。MirrorStore 实现 Journaling/WriteQuorum/LiveCount。
```

### 2.2 S3 适配实现要点

| 操作 | S3 API（经 minio-go/v7） | 说明 |
|---|---|---|
| Put | `PutObject(key, reader)` | 新对象创建，读后写一致 |
| Get(Range) | `GetObject` + `Range: bytes=off-end` | 按需取字节，不整对象拉取 |
| Head | `StatObject` | 取 ETag / 判断存在 |
| CreateIfAbsent | Head 预检 + `PutObject` 原子写 | 提交锁对象（单写者方案） |
| CAS | Head 预检 + `PutObject` 原子写 | 对 `current` 做"读-比-写"，见下方实测约束 |
| ListPrefix | `ListObjectsV2` + `StartAfter` | GC 列举（minio-go 无 ContinuationToken 字段） |
| Delete | `RemoveObjects(bucket, <-chan ObjectInfo)` | GC 批量删除 |

> **CAS 实测结论（minio-go v7.0.80 + RustFS 1.0.4）**：开发/测试 S3 兼容后端使用 **RustFS**（`rustfs server <datadir> --address 127.0.0.1:9000 --access-key ... --secret-key ...`，path-style），store 原语、Repo×S3 端到端、并发 CAS 均实跑通过。
> `PutObjectOptions.SetMatchETag` 会发送 `If-Match`，RustFS/MinIO/AWS S3 对单对象条件写是原子的：8 个并发 CAS（同一 expect ETag）实测只有 1 个成功，其余 `PreconditionFailed`（`internal/fs/store/s3store_integration_test.go: TestS3StoreCASConcurrent`）。因此 `current` 的条件推进与提交租约在单副本上成立，无需退化为"先读后写"。
> **FileStore 说明**：本地目录后端没有服务端条件写，`CAS` 在进程内互斥之外还持有 `flock`（`.cas.lock`），保证同一目录上的多个 svngod 进程/`svnadmin` 进程不会同时通过 ETag 检查并各自 rename（曾实测丢失更新）。
>
> **对象键命名**：仓库放同一 bucket 下的独立前缀 `<repo>/…`；仓库根 = `format`/`uuid`/`current` 所在层级。所有键用小写、数字定长补零（如 `revs-loose/0001234/000000004096`）保证字典序与列举顺序一致。

---

## 3. 数据模型与序列化

### 3.1 node-rev 语义

node-rev 是 SVN 版本树的不可变节点：一个 (修订, 节点) 产生一个 node-rev。文件含**属性**与**文本**（全文或增量）；目录含**属性**与**子项表**。

完整节点标识 = `(rev, node_id)`；`node_id` 为修订内递增的序号（在同一修订内唯一）。

### 3.2 node-rev 字节格式

数值一律 **uvarint**（对齐 Go `binary.Uvarint`）；字符串 = `uvarint 长度 + 原始字节`（记为 `[ulen s]`）。

```
node-rev record:
  kind          u8                    // 1=file, 2=dir
  node_id       uvarint               // 修订内节点序号
  pred_rev      uvarint               // 前驱修订（0 表示无）
  pred_id       uvarint               // 前驱 node_id
  delta_depth   uvarint               // 文本增量深度（见 §7，仅 file 有意义）
  has_copy      u8                    // 0/1
  if has_copy:
    copyfrom_rev   uvarint
    copyfrom_node_id uvarint
    copyfrom_path  [ulen s]
  props_rep     u8                    // 0=完整属性集, 1=相对 pred 的属性增量
  props_len     uvarint
  props         [props_len bytes]
  if kind == file:
    text_rep     u8                   // 0=全文, 1=svndiff 增量
    text_md5     [16]byte             // 文本 MD5（svnadmin verify 对齐）
    text_len     uvarint
    text         [text_len bytes]     // 全文 或 svndiff delta 字节
    if text_rep == 1:
      base_rev   uvarint              // 增量基（逻辑定位）
      base_node_id uvarint
  if kind == dir:
    num_children  uvarint
    for i in 0..num_children:
      name      [ulen s]              // 子项名（UTF-8）
      child_rev    uvarint            // 子项 node-rev 的修订
      child_node_id uvarint
  record_crc   u32                    // 记录体 CRC32（数据完整性）
```

> 说明：
> - 目录文本为**空**；目录的"内容"即 `num_children` 子项表。
> - `pred_*` 用于目录 `svn log`/`diff` 与属性增量；`copyfrom_*` 记录复制来源（分支/标签）。
> - `props` 序列化见 3.5；`record_crc` 覆盖除自身外全部字段字节。

### 3.3 rev_block（修订块）

一个修订的所有 node-rev 打包成一个 **rev_block**，这是"每修订一个不可变写入单元"的载体，同时用于**松散对象**与**分片包**（统一格式）。

```
rev_block:
  magic        [8]byte = "SVNGREV1"
  rev          u64                     // 本块修订号
  prev_rev     u64                     // rev-1（校验用）
  n_node_revs  u32
  node_table_len uvarint
  node_table   [node_table_len bytes]  // 见下
  payload      [ ... node-rev records ... ]
```

`node_table`：按 node_id 升序，每条 `(node_id, payload_offset, len)`，`payload_offset` 相对 payload 起点。读取一个 node-rev = 在块内查表得 (offset,len) → 取该段字节解析。

> **对齐 FSFS format 7 思想**：逻辑寻址（node-rev 用逻辑 id）+ 索引映射到物理位置。我们把"索引"拆成两级：**分片 idx 存 rev→块偏移**，**块内 node_table 存 node_id→段偏移**，两级都缓存。

### 3.4 松散对象与分片包

- **松散对象** `revs-loose/<shard>/<N>`：恰好一个 rev_block（同格式）。
- **分片包** `revs/<shard>.pack`：该分片全部 rev_block 按 rev 升序拼接。
- **分片 idx** `revs/<shard>.idx`：

```
shard idx:
  magic       [8]byte = "SVNGIDX1"
  shard       u64
  first_rev   u64
  count       u32
  rev_table   [count * 16 bytes]       // 每项：block_offset u64, block_len u64
  crc32       u32
```

读取 `(rev, node_id)`：`shard = rev / SHARD_SIZE`；`idx.rev_table[rev-first_rev]` → block_offset/len → **一次 Range 读**取整块 rev_block → 块内 node_table 定位 node-rev。

### 3.5 revprops 序列化

```
revprops payload:
  n      uvarint
  for i in 0..n: name [ulen s], value [ulen s]
```

- 松散 `revprops-loose/<shard>/<N>`：单条修订属性。
- 分片包 `revprops/<shard>.pack`：该分片全部修订属性拼接（每条前加 `u64 rev + uvarint len` 便于随机读）。

### 3.6 current / format / uuid

```
current（CAS 目标）:
  "SVNGHEAD1\n"
  "HEAD=<N>\n"
  "CREATED=<unix>\n"

format（仓库一次性写入）:
  "SVNGOBJ1\n"
  "FORMAT=1\n"
  "SHARD_SIZE=<N>\n"
  "MAX_DELTA_CHAIN=<N>\n"

uuid: 随机 UUID 文本（与 SVN 仓库语义一致，dump 头携带）。
```

`format` 一经写入不可变；`SHARD_SIZE`/`MAX_DELTA_CHAIN` 在建仓时固化，杜绝运行期不兼容。

### 3.7 提交 WAL（journal/<N>）

```
偏移  长度  内容
0     8     "SVNGWAL1"
8     4     JSON 长度（uint32 大端）
12    4     CRC32C（Castagnoli，覆盖 JSON）
16    N     JSON:
           { "v":1, "rev":N, "parent":N-1, "lease":"<token>",
             "created":<unix>, "revblock":"revs-loose/.../<N>",
             "revblock_sha256":"<hex>", "props":"revprops-loose/...",
             "author":"...", "log":"...", "quorum":2 }
```

- 解码时校验魔数、长度、CRC；任一不符即拒绝回放（视为损坏记录）。
- 记录在 `current` CAS 之前写入；提交确认前，严格模式要求记录在 N 份副本上存在。
- 回放规则：记录达到法定人数（可用性模式为 1）、revblock 在法定人数上存在且 SHA-256 匹配、`parent == HEAD` → 把 `current` 前滚到 N。否则视为中止意图，交 GC。

---

## 4. 对象布局与生命周期

```
<repo>/
  format / uuid / current
  revs/<shard>.pack / .idx / .done          # 已封装修订分片 + 打包标记
  revprops/<shard>.pack                     # 已封装修订属性
  revs-loose/<shard>/<N>                    # 活跃分片松散修订对象
  revprops-loose/<shard>/<N>
  txn/<txid>/                               # 进行中事务暂存
  locks/commit                              # 跨进程提交租约（TTL 20s，心跳续约）
  journal/<N>                               # 提交 WAL（CRC32C + revblock SHA-256）
  dav-activities/<uuid>                     # 活动→事务映射
  dav-locks/locks.json                      # DAV 文件锁表（CAS）
```

### 4.1 分片状态机

每个 shard 三态：`LOOSE`（活跃写入）→ `PACKING` → `PACKED`（`revs/<s>.done` 已写）。

- 读路径判定（结果缓存）：`PACKED` → 走 `revs/<s>.pack`+`.idx`；否则走 `revs-loose/<s>/<N>`。
- **打包标记 `.done` 是权威切换点**：写 `.done` 前，pack 即使存在也不被引用（属孤儿）；写 `.done` 后，松散对象转为不可达（交 GC）。

---

## 5. 写路径（提交）实现

### 5.1 事务暂存（DeltaV 活动）

客户端经 MKACTIVITY/CHECKOUT/PUT/DELETE/COPY/MOVE 产生的变更，在协议层归并为一次事务 `txn/<txid>/`：

1. `MKACTIVITY` → 分配 `txid`，写 `dav-activities/<uuid>` 记录（活动→事务、创建时间）。
2. 变更逐条应用：目录结构变更（add/delete/copy/move/mkdir）与文件变更（PUT 全文/svndiff）先写入 `txn/<txid>/changes.pack`（临时累积，格式同 rev_block 的 node-rev 序列）。
3. 暂存期间不触碰任何 `revs*` 对象，不推进 `current`。

### 5.2 提交固化（MERGE 触发）

```
func (r *Repo) CommitTxn(txid, newRevProps) (newRev, error):
  1  lease := acquireCommitLease(locks/commit)  // TTL 20s，心跳续约
     defer lease.Release()
        - 可用性模式：CreateIfAbsent 写"当前可达的全部副本"；单节点故障降为 1 份
        - 严格模式：写固定法定人数（如 3 节点 quorum=2）
  2  reconcileJournal()                     // 回放 WAL：把 quorum 已确认但 HEAD 未推进的提交前滚
  3  cur := readCurrent()                   // HEAD = cur.Head（可能刚被前滚）
  4  N  := cur.Head + 1
     shard := N / r.shardSize
  5  block := buildRevBlock(N, txn)         // 组装 node-revs + dir 树遍历补全隐式变更
  6  Put(revs-loose/<shard>/<N>, block)     // 数据对象（quorum 模式下按 quorum 写）
  7  Put(revprops-loose/<shard>/<N>, props)
  8  Put(journal/<N>, WALRecord(...))       // CRC32C；严格模式要求 quorum 份
  9  lease.Verify()                          // 防止租约在做完 5-8 后已失效
 10  newETag := CAS(current, cur.ETag, "HEAD=N")   // 唯一发布点
     if ErrPreconditionFailed:  delete staged; retry from 3
 11  Delete(txn/<txid>/...)                 // 清理暂存
 12  if N % shardSize == shardSize-1: enqueuePack(shard)
 13  return N
```

> **为什么先写数据后 CAS**：第 5/6 步写对象仅"存在"，第 7 步 CAS 成功才"可见"。崩溃于 5/6 后、7 前 → `current` 仍为 N-1，孤立的 `revs-loose/<shard>/<N>` 交 GC；仓库一致性无损。

### 5.3 崩溃恢复场景表

| 崩溃点 | 结果 | 恢复 |
|---|---|---|
| 事务暂存中 | `txn/<txid>` 残留 | GC 按 TTL 清理 |
| 写 loose 对象后、WAL 前 | 孤儿 loose 对象 | GC 清理；HEAD 不变 |
| WAL 写入后、CAS 前 | 记录可能存在（可达法定人数） | 打开仓库/下次提交回放：revblock 哈希匹配且 parent==HEAD → **前滚**；否则视为中止意图交 GC |
| WAL 只写到少数副本 | 不构成已确认提交 | 回放要求法定人数（严格）或最新副本（可用性），其余交 GC |
| CAS 后、清 txn 前 | 修订已提交，txn 残留 | GC 清理 txn |
| 封装中（写 pack/idx 后、写 .done 前） | 孤儿 pack | 读路径仍用 loose；GC 清理无 .done 的 pack |
| 封装写 .done 后 | 已切换 | 读走 pack；loose 交 GC |

### 5.4 多进程与法定人数

- **租约**：`locks/commit` 的创建/CAS/删除在 MirrorStore 上按"配置顺序串行"执行：第一个可达节点是仲裁者，任一节点报告已存在即整体失败并回滚已创建副本。顺序执行避免了"两个竞争者各赢一个节点、再互相回滚"的活锁（实测并发提交卡死）。
- **可用性模式**（默认，不配 `quorum`）：租约要求"当前可达的全部副本"；两节点都在时两个 svngod 进程无法各持一份，单节点故障时降为一份继续提交。
- **严格模式**（`quorum: N`）：数据、WAL、租约都要求 N 份；不足即 `ErrQuorumUnmet` → HTTP 503，HEAD 不推进。
- **跨进程读收敛**：`HeadFresh` 以 500ms 节流回放 WAL；fenced 状态下读取所有可达副本并取最新 HEAD，绝不返回比已确认值更旧的指针；副本 HEAD ≥ 已确认 HEAD 即可采纳（否则另一个进程提交后本进程会永久 fencing）。
- **repair 作用域**：`WithRepoPrefix("<storage.prefix>/<name>")` 使 repair 只列举/复制本仓库对象；配置的前缀必须同时传给 `OpenRepo` 与 MirrorStore（`cmd/svngod`、`svnadmin --mirror-file` 已贯通），否则 repair 会扫错前缀并把副本误判为完整。
- **revprop 更新**：镜像后端把 revprop 修改纳入提交租约（与提交串行），读取取多数派、写入广播到全部可达副本（`PutAll`），避免两个进程各自 CAS 不同主节点导致属性丢失或读到旧值。
- **WAL 保留与崩溃注入**：`svnadmin gc` 保留最近 4096 条 journal 记录（更早的清理）；裁剪按精确键范围并发删除，不列举整个 journal 前缀（625k 修订的仓库需要 625 个 list 页，RustFS 上会卡住数小时）。打包完成后，loose rev/revprops 由后台异步回收，且**只删主副本**（`DeletePrimary`）：备副本上的残留无害（修复只列活动分片），而成批删除镜像到备节点会在 8 秒预算内超时、把备节点踢下线。实测 RustFS 单批 1000 键删除约 28 秒，8 并发约 90 键/秒，单仓全量 GC（loose+journal）约 4–6 小时。`SVNGOD_CHAOS_AFTER_WAL=1` 是测试专用钩子，在 WAL 落盘、`current` CAS 之前退出进程，用于实测回放前滚（生产不设置即无影响）。
- **回放与在线提交的竞争**：WAL 回放不持有提交租约。另一个进程的回放可能在本进程 `current` CAS 之前就发布了同一条记录；此时本进程 CAS 得到 `PreconditionFailed`。只要 `current.Head == N`、记录中的 `lease` 等于本进程令牌、`parent` 与读取时一致、`revblock_sha256` 等于本进程写入的块，就把提交视为成功（修订已按本进程的字节发布），避免向客户端返回 409 让它在“已提交”的变更上重试并被判过期。
- **租约抖动容错**：租约读取/续约遇到后端瞬时错误（连接重置、S3 5xx）不再立即判定“租约丢失”，而是重试 3 次（间隔 100ms）；只有读到他人的令牌或对象不存在才算丢失。持久失败返回 `ErrLeaseUnverified` → HTTP 503（可重试）。若失败发生在 WAL 已落盘之后，且 `journal/<N>` 仍是本进程的记录（令牌与 revblock SHA-256 都匹配），则继续推进 `HEAD`（回放本来也会发布它），不会把已预留的修订报成失败。
- **提交吞吐实测（2026-11，主备 RustFS，LAN）**：1KiB 单对象 Put 30.7ms、Get 1.1ms；单仓库一次提交 220–280ms，分解为租约 12–34ms、revblock+revprops 47–78ms、WAL 32–36ms、HEAD CAS 65–95ms。这是**未开组提交**的基线：单仓库约 4 次/秒，16 个仓库并发合计 13.7 次/秒（后端写吞吐先饱和，横向扩展收益递减），1000 万次约需 202 小时。
- **批量提交（group commit，`Repo.CommitBatch`）**：一个租约周期内构建多个修订，对象的 revblock/revprops/WAL 并发写入，最后用一次 `current` CAS 发布整批；客户端只在本批 CAS 成功后收到成功响应，崩溃时未发布的修订不会返回成功。实测单进程 4 → 33 次/秒（K=64），4 进程 45.7 次/秒，8 进程 48.8 次/秒；16 个并发进程反而降到约 29 次/秒（后端小对象写饱和）。16 仓 × 625,000 修订的千万级长跑使用 8 个 worker，实测可持续 37–48 次/秒，约 60 小时完成。该 API 同时支持逐路径 baseRev 检查：在线 DAV 提交由每个仓库的协调器收集（`-commit-batch`，默认 16；`-commit-window-ms`，默认 2），在内存树上按“节点修订号 > base”判定冲突。原生 svn 客户端的瓶颈是每次提交的会话往返（MKACTIVITY/PUT/MERGE），8 并发下组提交只能把单仓库 HTTP 提交从 ~3.9 提升到 ~4.5 次/秒；批量写入（`cmd/revscale`）才能达到 30+ 次/秒。
- 再往上的瓶颈是每个修订仍要写 revblock/revprops/WAL 三个小对象（后端小对象写约 200 次/秒）。要做到小时级，需要把一批修订合并进一个 segment 对象、由一次写发布多个修订（格式升级），或使用 fsync 延迟更低的介质/本地 WAL。
- **修复列举的不变量（2026-11 千万级压测修复）**：repair 必须精确列举控制键（`current`/`format`/`uuid`，不能给前缀强补 `/`）；S3 适配器列目录时保留尾部斜杠（列 `revs/` 不能同时命中 `revs-loose/`）；loose 对象只列**活动分片**（已完成分片由 pack 提供）；可达性探测读 `current` 单键而不是列整个仓库；列表后被删除的瞬时对象（如 `locks/commit.break`）按“已消失”跳过，不能把活着的备节点标记失败；提交租约键不进入修复范围。违反任一条都会让备节点在几万修订的仓库上反复超时/被踢下线，实测可落后 2 万修订。分片打包后留下的 loose 副本由 `svnadmin gc` 回收，不在提交路径镜像批量 DELETE（否则 8 秒的备写预算会被大批删除耗尽，HEAD 停止复制）。
- **备节点长期离线**：可用性模式继续在主节点提交（单副本窗口，原子性不变）；恢复后 `svnadmin repair` 按 pack 补齐。修复积压以 4096 修订/包的粒度增长，量级远小于逐修订复制。
- **锁状态只认仲裁者**：两节点主备下，`locks/commit`、`locks.json` 只在第一项（仲裁者）上创建/删除，备节点可能残留更新前的旧副本。修复遍历不得把“只存在于备节点”的锁状态回灌到仲裁者——实测这样会把一张已释放的租约复活，使正在提交的进程读到别人的令牌而报 `commit lease lost`。规则：仲裁者可达时，有则从仲裁者复制到备，无则删除备上的残留；仲裁者不可达时不动锁状态。

---

## 5.9 Segment 提交格式（v2，进行中）

为把对象数从 O(revisions) 降到 O(revisions/batch) 并实现在线 compaction，新增可选的 segment 格式：

- `segs/<shard>/<first>-<count>.seg`：一批修订的 revblock+revprops，带 CRC32C；
- `segs/<shard>.idx`：分片内有序索引（first/count/key/SHA-256），读路径二分定位；
- 每批一条 `journal/<first>` WAL（v2），回放按批前滚；
- `format` 增加 `SEGMENTS` / `SEGMENT_FROM` / `SEGMENT_TARGET`，v1 仓库不受影响。

设计、提交协议、崩溃窗口、compaction 与迁移步骤见 [`存储引擎Segment格式设计.md`](存储引擎Segment格式设计.md)。当前已落地格式字段、键布局、segment 编解码与索引（含单测）；提交/读取/compaction 接线与迁移工具按文档阶段推进。

## 6. 读路径实现

```
func (r *Repo) GetNodeRev(rev, nodeId) (*NodeRev, error):
  shard := rev / r.shardSize
  block := r.block(rev, shard)              // 见下，结果缓存
  e := block.nodeTable[nodeId]
  return parseNodeRev(block.payload[e.off:e.off+e.len])

func (r *Repo) block(rev, shard) (*RevBlock, error):
  if r.state(shard) == PACKED:               // 缓存 shard 状态
     idx := readShardIdx(shard)              // 缓存
     off, ln := idx.revTable[rev - idx.firstRev]
     data := Get(revs/<shard>.pack, Range{off, ln})   // 一次 Range
     return parseRevBlock(data)
  else:
     data := Get(revs-loose/<shard>/<rev>)            // 整块
     return parseRevBlock(data)
```

### 6.1 文本重构

```
func (r *Repo) GetText(rev, nodeId) ([]byte, error):
  nr := GetNodeRev(rev, nodeId)
  if nr.text_rep == FULLTEXT:
     return nr.text
  // 增量：沿链向上到最近全文基
  base := r.GetText(nr.base_rev, nr.base_node_id)     // 递归，结果缓存
  return svndiff.Apply(base, nr.text)                  // 校验 MD5
```

- **链长有界**：由 §7 的提交时全量化策略保证任何 `(rev,node)` 到最近全文基的增量跳数 ≤ `MAX_DELTA_CHAIN`。
- **缓存**：全文结果按 `(rev, node_id)` 入 LRU；`GET` 文件内容、`PROPFIND` 目录、`svn update` 大量命中。

---

## 7. 增量策略与 deltification

**约束**：不可变对象无法改写历史修订，故**不在后台改写历史**，而是在**提交时决策**全量还是增量。

```
决策（提交文件 node-rev 时）：
  base := 候选基（同路径 pred，或 copyfrom 源，选 delta_depth 更小者）
  if base.delta_depth + 1 >= MAX_DELTA_CHAIN:   // 默认 64
      text_rep = FULLTEXT ; delta_depth = 0
  else:
      text_rep = DELTA ; delta_depth = base.delta_depth + 1
      text = svndiff.Encode(base.fulltext, newText)
```

- 任何新 node-rev 的 `delta_depth ≤ MAX_DELTA_CHAIN-1`，因此 HEAD 处读文件全文最多应用 `MAX_DELTA_CHAIN` 次 svndiff，且每次沿链向上都命中缓存基文本。
- 历史冷读（checkout 老修订）链长可能更长，但属于冷路径，接受更长延迟（仍以缓存缓解）。
- `delta_depth` 随 node-rev 落盘（见 3.2），无需在线回溯统计。

> 与传统 SVN "deltify/包装时压缩历史"不同，本设计以**提交时全量化**替代历史重写，换得不可变对象下的可行性；空间略增（每 64 次修改写一次全文），换取时间有界。

---

## 8. svndiff 编解码（字节级）

SVN 官方二进制增量格式（`svndiff`），客户端上报与历史存储都用它。
> 以下格式已对照 Apache SVN trunk 官方实现 `libsvn_delta/svndiff.c` 逐字段核验，
> 与仓库代码 `internal/svndiff/svndiff.go` 严格一致。

```
文件   = 头 + 若干窗口
头     = 'S' 'V' 'N' 版本字节      // 0x00=SVNDIFF_V0（本实现），0x01=v1(zlib)，0x02=v2(lz4)
窗口   = 窗口头 + 指令段 + 数据段   // 无 src_ops 计数、无窗口尾 CRC32
```

### 8.1 窗口头（5 个编码整数，均 uvarint）

| 顺序 | 字段 | 说明 |
|---|---|---|
| 1 | sview_offset | 源视图在源全文中的偏移 |
| 2 | sview_len | 源窗口字节数 |
| 3 | tview_len | 目标窗口产出字节数 |
| 4 | ins_len | 指令段字节数 |
| 5 | data_len | 数据段字节数 |

> 窗口头**没有**独立的 src_ops 计数；source 拷贝操作数由解码器扫描指令段时统计得出。

### 8.2 指令流（无 src_ops）

每条指令 = 1 个起始字节 opcode：

- **高 2 位**：action 选择器 `00`=copy_from_source、`01`=copy_from_target、`10`=copy_from_new（insert）、`11`=保留。
- **低 6 位**：长度。`0` 表示"超长"，长度随后用 append 扩展编码给出（见 8.3）。
- 指令操作数：
  - source：其后跟 1 个 uvarint **offset**（源窗口内偏移）。
  - target：其后跟 1 个 uvarint **offset**（目标窗口已产出区偏移，允许重叠）。
  - new：**不带** offset，直接从数据段顺序取字节插入。

### 8.3 长度 append 编码（对齐 `svn__encode_uint`）

| 值范围 | 编码 |
|---|---|
| `0x00 ≤ v < 0x80` | 1 字节，原值 |
| `0x80 ≤ v < 0x4000` | 2 字节：`0x80 | (v>>8)`，`v & 0xff` |
| `0x4000 ≤ v < 0x200000` | 3 字节：`0xc0 | (v>>16)`，… |
| `0x200000 ≤ v < 0x10000000` | 4 字节：`0xe0 | (v>>24)`，… |
| 更大 | 5 字节：`0xf0 | (v>>32)`，… |

### 8.4 解码校验（与官方 `count_and_verify` 等价）

- 指令长度 `len==0` 一律拒绝。
- source：`offset+len` 不得越出源窗口 `sview_len`。
- target：`offset < tpos`（不得引用未产出区）。
- new：当前数据段指针 `+len` 不得越出 `data_len`。
- 收尾：`tpos` 必须恰好填满 `tview_len`；数据段指针必须恰好耗尽 `data_len`。
- 任一校验失败即报错，绝不部分产出。

### 8.5 Go 接口

```go
package svndiff
func Encode(src, tgt []byte) ([]byte, error)   // 产出窗口序列
func Apply(src, delta []byte) ([]byte, error)  // 应用窗口序列，校验边界与耗尽
func IsDelta(data []byte) bool                 // 头 'SVN\x00'
```

> 解码务必做：指令长度非零、源/目标窗口边界检查、target offset<tpos、new 不越出数据段、`tpos` 填满 `tview_len` 且数据段耗尽（对齐官方 `count_and_verify`）。以官方 `svn` 生成的样本做往返（encode→apply 逐字节一致）与边界用例测试。

---

## 9. 缓存层

| 缓存 | 键 | 值 | 默认预算 | 说明 |
|---|---|---|---|---|
| shardState | (repo, shard) | PACKED/LOOSE | 小 | 避免每次判 packed |
| shardIdx | (repo, shard) | idx 字节 | 64 MiB | rev_table 热读 |
| revBlock | (repo, rev) | rev_block 字节 | 256 MiB | PROPFIND/update 命中 |
| fulltext | (repo, rev, node_id) | 文本字节 | 512 MiB | GET 文件、diff 主命中 |
| revprop | (repo, rev) | props | 64 MiB | log 命中 |

- 全部 LRU + 并发安全；总预算默认 1 GiB（config `storage.cache.memory_bytes`）。
- **命中率是第一性能指标**：P4 压测以 `cache_hit_rate`、`s3_round_trips_per_request` 为核心指标。
- 可选磁盘缓存（默认关）：把 fulltext/revBlock 落到本地，进一步减 S3 往返。

---

## 10. 分片封装任务（pack job）

```
func enqueuePack(shard):
  # 后台，可断点续跑
  1 revs := 按序读取 revs-loose/<shard>/*  的 rev_block（范围已知 [first, last]）
  2 拼接为 revs/<shard>.pack（记录每块 offset/len）
  3 写 revs/<shard>.idx
  4 写 revprops/<shard>.pack
  5 写 revs/<shard>.done                      # 权威切换点
  6 删除 revs-loose/<shard>/*、revprops-loose/<shard>/*（或交 GC）
```

- 步骤 2-4 的 pack/idx/props 在写 `.done` 前均为"孤儿"，读路径不受影响（仍走 loose）。
- 封装期间对**历史**分片无并发写（只读），无需与读加锁；对活跃分片仅在写满时触发一次。

---

## 11. 垃圾回收（GC）

**可达根**：`current`、`format`、`uuid`、全部 `revs/<s>.pack/.idx/.done`、`revprops/<s>.pack`、当前活跃分片的 `revs-loose/<s>/*`、`locks/*`、存活 `dav-activities/*`、保留窗口内的 `journal/*`。

**清扫目标**：
1. `txn/<txid>` 超过 TTL（默认 7 天）。
2. 已 `PACKED` 分片的 `revs-loose/<s>/*`、`revprops-loose/<s>/*`。
3. 无 `.done` 的孤儿 `revs/<s>.pack/.idx`、`revprops/<s>.pack`（封装中断残留）。
4. 孤儿 `revs-loose/<s>/<N>`（提交中断残留）。

**算法**：
```
for shard in 0..activeShard:            # 前缀分片 + 游标续列
  if has .done:  delete revs-loose/<shard>/*, revprops-loose/<shard>/*
  else:          if full && not packing: enqueuePack(shard)   # 补齐封装再回收
cleanup stale txn/*, orphan dav-activities/*
```

- `ListPrefix` 分片前缀 + `ContinuationToken`，批量 `DeleteObjects`（每批 ≤ 1000）。
- 只删**确定不可达**对象；弱一致 S3 下删除延迟不影响正确性（根数据不可变）。

---

## 12. 校验（svnadmin verify 对齐）

- 写路径：node-rev 的 `record_crc`、文本 `MD5` 落盘即校验。
- 读路径：解析后校验 `record_crc`；重构文本后比对 `text_md5`。
- `svngod-admin verify <repo>`：遍历全部分片/松散块 → 逐 node-rev 校验 → 校验 `current` 与 shard idx 一致性 → 校验 delta 链闭合。
- `svnadmin journal -n N <repo>`：解析 WAL 尾部（魔数/长度/CRC32C），输出 rev、parent、quorum、revblock SHA-256 与 log；损坏记录直接拒绝。

---

## 13. Go 类型与包结构

```
internal/fs/store/
  store.go        // Store / QuorumRegister 接口、ErrQuorumUnmet
  filestore.go    // 本地目录后端（CAS 跨进程 flock）
  s3store.go      // S3 适配（minio-go，If-Match 条件写）
  mirror.go       // 主备镜像：quorum 写、租约寄存器、健康/修复/故障切换
  keys.go         // 对象键构造/解析
  format.go       // format/uuid/current 读写
  idx.go          // shard idx 编解码
  block.go        // rev_block 编解码
  noderev.go      // node-rev 编解码
  revprop.go      // revprops 编解码
  pack.go         // 分片封装任务
  gc.go           // 垃圾回收
internal/fs/
  repo.go         // Repo：CommitTxn / GetNodeRev / GetText / GetDir / HeadFresh 对账
  journal.go      // 提交 WAL：帧编码、CRC32C、回放/前滚、JournalTail
  lease.go        // 跨进程提交租约：TTL/心跳、法定人数/可用性两种模式
  txn.go          // 事务暂存
  tree.go         // 目录树遍历/隐式变更补全
internal/svndiff/
  svndiff.go      // Encode / Apply / IsDelta
internal/cache/
  lru.go          // 并发安全 LRU
```

关键类型：

```go
type NodeID struct{ Rev uint64; ID uint64 }

type NodeRev struct {
    Kind           byte            // 1 file, 2 dir
    NodeID, Pred   NodeID
    DeltaDepth     int
    CopyFrom       *NodeID
    CopyFromPath   string
    Props          map[string]string
    TextRep        byte            // 0 full, 1 delta
    TextMD5        [16]byte
    Text           []byte
    Base           *NodeID         // 增量基
    Children       []Child         // 仅 dir
}
type Child struct{ Name string; ID NodeID }

type Repo struct {
    store       Store
    prefix      string             // 仓库前缀
    shardSize   uint64
    maxDelta    int
    writeQuorum int                // >1 严格模式；0/1 可用性模式
    journal     bool               // mirror 后端启用提交 WAL
    cache       *cache.LRU
    mu          sync.Mutex         // 进程内提交串行化
}

func (r *Repo) CommitTxn(tx *Txn, revProps map[string]string) (uint64, error)
func (r *Repo) GetNodeRev(id NodeID) (*NodeRev, error)
func (r *Repo) GetText(id NodeID) ([]byte, error)
func (r *Repo) GetDir(id NodeID) ([]Child, error)
func (r *Repo) Verify(ctx context.Context) error
```

---

## 14. 实现要点与风险

| 项 | 风险/注意 | 对策 |
|---|---|---|
| CAS 跨提供商 | 条件写能力因 SDK/服务而异 | `PutObjectOptions.SetMatchETag`（If-Match）在 RustFS/MinIO 实测原子（8 并发仅 1 胜）；FileStore 用 flock 保证跨进程 |
| svndiff 解码安全 | 恶意/损坏 delta 越界、非法 action | 严格边界与耗尽校验（对齐 `count_and_verify`），拒绝即报错不崩溃；无窗口尾 CRC |
| 提交时全量化成本 | 大文件每 64 次改一次全文，写放大 | MAX_DELTA_CHAIN 可配；与文本体积权衡 |
| 封装/GC 并发 | 与读路径无冲突（历史只读）；与提交在活跃分片边界 | `.done` 权威切换 + 只回收确定不可达 |
| 1000 万修订列举 | ListObjects 慢 | 前缀分片 + 游标续列，分片粒度批量 |
| 缓存抖动 | 热点切换导致 S3 打满 | 分预算 + 命中率告警 + 磁盘缓存兜底 |
| 大目录块 | 单个 rev_block 很大（万级子项） | 分片 idx 用 Range 读整块；子项表分页可选优化 |
| 多进程/分区 | 两节点分区时可用性与一致性不可兼得 | 默认可用性 + WAL 对账；强一致用 3 节点 `quorum:2`；租约多数派相交 |
| WAL 记录增长 | 每提交一个小对象 | 保留窗口清理；`svnadmin journal` 审计；单条记录 KB 级 |

---

*参考：Subversion 官方 svndiff 实现（libsvn_delta/svndiff.c）与格式讨论（svn-haxx 提案）；FSFS format 7 逻辑寻址与双索引（Apache cwiki/release notes）；RFC 4918/3253（WebDAV/DeltaV 由协议层承担）。*
