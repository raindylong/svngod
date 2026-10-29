# svngod — S3 存储引擎实现细则（实现级设计）

> 配套文档 | 归属：《Go-SVN服务器架构设计方案》第 5 章 | 版本 v1.1 | 2026-10-28
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
    // 创建不可变对象（写新 key）。调用方保证 key 唯一且内容一次成型。
    Put(ctx context.Context, key string, data []byte) error

    // 读取对象全部或 Range 字节。
    Get(ctx context.Context, key string, rng *Range) ([]byte, error)
    Head(ctx context.Context, key string) (etag string, exists bool, err error)

    // 条件创建：仅当 key 不存在时创建成功（提交锁）。
    // 返回 ErrPreconditionFailed 表示已存在。
    CreateIfAbsent(ctx context.Context, key string, data []byte) error

    // 原子推进 current：仅当当前 ETag==expect 时覆盖为新内容（CAS）。
    // 返回新 ETag；expect 失配返回 ErrPreconditionFailed。
    CAS(ctx context.Context, key string, expectETag string, data []byte) (newETag string, err error)

    // 按前缀列举（分页 + 游标续列），供 GC。
    ListPrefix(ctx context.Context, prefix string, continuation string, max int) (objs []ListedObj, nextContinuation string, err error)

    // 批量删除（GC）。
    Delete(ctx context.Context, keys []string) error
}
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

> **CAS 实测约束（minio-go v7.0.80，已在本工程验证）**：开发/测试 S3 兼容后端已切换为 **RustFS**（`rustfs server <datadir> --address 127.0.0.1:9000 --access-key test --secret-key test12345678`，仅 path-style），S3 集成测试（store 原语 + Repo×S3 端到端）在 RustFS 上实跑全通过、代码零改动。
> `PutObjectOptions` 无 `If-Match`/`If-None-Match` 条件写字段；`CopyObject(ctx, CopyDestOptions, CopySrcOptions)` 的 `MatchETag` 虽映射 `x-amz-copy-source-if-match`，但 **CopyObject 自拷贝只能改元数据、不能改写对象 body**，无法承载条件写语义。因此在"单进程 + 提交互斥锁"的既定约束下，`Store.CAS`/`CreateIfAbsent` 退化为 **Head 预检 + PutObject 原子写**：S3 读后写一致 + 进程内互斥，正确性成立；多写者并发时可能出现"预检通过后他人已改"的丢失更新。**真正的 S3 条件写需 AWS SDK v2 的 `If-Match`/`If-None-Match` 条件 Put，已列为升级路径**（当前架构本就单进程单实例，不构成阻塞）。
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
  locks/commit                              # 提交互斥锁对象
  dav-activities/<uuid>                     # 活动→事务映射
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
  1  lock := r.commitLock()                 // 进程互斥 + CreateIfAbsent(locks/commit) 双保险
     defer lock.Unlock()
  2  cur := readCurrent()                   // HEAD = cur.Head
  3  N  := cur.Head + 1
     shard := N / r.shardSize
  4  block := buildRevBlock(N, txn)         // 组装 node-revs + dir 树遍历补全隐式变更
  5  Put(revs-loose/<shard>/<N>, block)     // 1 次 PUT
  6  Put(revprops-loose/<shard>/<N>, props) // 1 次 PUT
  7  newETag := CAS(current, cur.ETag, "HEAD=N")   // 原子推进
     if newETag == ErrPreconditionFailed:   // 理论不发生于单进程，防御
        delete staged; retry from 2
  8  Delete(txn/<txid>/...)                 // 清理暂存
  9  if N % shardSize == shardSize-1:       // 本片已写满
        enqueuePack(shard)                  // 触发封装任务（异步）
 10  return N
```

> **为什么先写数据后 CAS**：第 5/6 步写对象仅"存在"，第 7 步 CAS 成功才"可见"。崩溃于 5/6 后、7 前 → `current` 仍为 N-1，孤立的 `revs-loose/<shard>/<N>` 交 GC；仓库一致性无损。

### 5.3 崩溃恢复场景表

| 崩溃点 | 结果 | 恢复 |
|---|---|---|
| 事务暂存中 | `txn/<txid>` 残留 | GC 按 TTL 清理 |
| 写 loose 对象后、CAS 前 | 孤儿 loose 对象 | GC 清理 |
| CAS 后、清 txn 前 | 修订已提交，txn 残留 | GC 清理 txn |
| 封装中（写 pack/idx 后、写 .done 前） | 孤儿 pack | 读路径仍用 loose；GC 清理无 .done 的 pack |
| 封装写 .done 后 | 已切换 | 读走 pack；loose 交 GC |

---

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

**可达根**：`current`、`format`、`uuid`、全部 `revs/<s>.pack/.idx/.done`、`revprops/<s>.pack`、当前活跃分片的 `revs-loose/<s>/*`、`locks/*`、存活 `dav-activities/*`。

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

---

## 13. Go 类型与包结构

```
internal/fs/store/
  store.go        // Store 接口
  s3store.go      // S3 适配（minio-go）
  keys.go         // 对象键构造/解析
  format.go       // format/uuid/current 读写
  idx.go          // shard idx 编解码
  block.go        // rev_block 编解码
  noderev.go      // node-rev 编解码
  revprop.go      // revprops 编解码
  pack.go         // 分片封装任务
  gc.go           // 垃圾回收
internal/fs/
  repo.go         // Repo：CommitTxn / GetNodeRev / GetText / GetDir
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
    store     Store
    prefix    string               // 仓库前缀
    shardSize uint64
    maxDelta  int
    cache     *cache.LRU
    commitMu  sync.Mutex
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
| CAS 跨提供商 | 条件写能力因 SDK/服务而异 | 单进程下 Head+Put 单写者方案已满足；多写者条件写走 AWS SDK v2 升级路径，RustFS/MinIO 集成测试均通过 |
| svndiff 解码安全 | 恶意/损坏 delta 越界、非法 action | 严格边界与耗尽校验（对齐 `count_and_verify`），拒绝即报错不崩溃；无窗口尾 CRC |
| 提交时全量化成本 | 大文件每 64 次改一次全文，写放大 | MAX_DELTA_CHAIN 可配；与文本体积权衡 |
| 封装/GC 并发 | 与读路径无冲突（历史只读）；与提交在活跃分片边界 | `.done` 权威切换 + 只回收确定不可达 |
| 1000 万修订列举 | ListObjects 慢 | 前缀分片 + 游标续列，分片粒度批量 |
| 缓存抖动 | 热点切换导致 S3 打满 | 分预算 + 命中率告警 + 磁盘缓存兜底 |
| 大目录块 | 单个 rev_block 很大（万级子项） | 分片 idx 用 Range 读整块；子项表分页可选优化 |

---

*参考：Subversion 官方 svndiff 实现（libsvn_delta/svndiff.c）与格式讨论（svn-haxx 提案）；FSFS format 7 逻辑寻址与双索引（Apache cwiki/release notes）；RFC 4918/3253（WebDAV/DeltaV 由协议层承担）。*
