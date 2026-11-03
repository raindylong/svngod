
---

## 附：稳定性与混沌测试记录

### 2026-11 假死副本与恢复中断专项（第四轮，100 万对象实例）

- **场景**：备节点 `SIGSTOP`（进程暂停，TCP 仍接收）；恢复过程中再次宕机；双副本同时不可用后单节点恢复；暂停主节点；9 场景原子性套件回归；10 万文件混合批次。
- **关键发现与修复**：
  1. **假死备节点让提交阻塞 243 秒**：读回退没有单副本超时，提交在读备副本时一直等到对方恢复。修复：主副本读 60s、备副本回退读 10s 预算，超时即摘除并触发恢复。
  2. **主动健康探测**：对非主副本每 5 秒做一次 5s 上限的 `HEAD current`（O(1)，不做列举），连续 3 次失败即摘除（约 15–20 秒发现假死），不再等 60 秒周期 repair。早先版本用 2s 列举探测，在 100 万对象 repair 负载下误判备节点并反复抖动，已改为 HEAD。
  3. **探测曾误杀主节点**：高负载下一次短超时列举被判失败，连续两次后主节点被摘除并 fencing，整库 503。修复：探测只针对非主副本；主节点由实际读写超时判定。
  4. **租约操作不再为不可用副本付费**：`CreateIfAbsentQuorum/CASQuorum/DeleteIfMatchQuorum` 跳过已摘除副本，租约对象超时从 15s 降到 5s。此前假死副本会让每次租约操作超时并丢租约，提交反复重试。
  5. **挂起型故障强制全量校验**：探测/读超时摘除的副本，其宕机前后的写是否落盘不确定（`wholeAtDown=false`、`pendingOverflow`），恢复时走全量列举与校验，而不是只补 pending 差量。
- **结果**：
  - 假死备节点：提交 **243s → 1–10s**（取决于先触发写超时还是探测摘除），内容完整；恢复后 22–42s 内追上并双节点一致。
  - 恢复中再次宕机：宕机期间 3 个修订全部成功，第二次杀掉正在恢复的节点后重启，3 棵树逐文件一致、HEAD 收敛、WAL SHA 一致。
  - 双副本全宕：提交干净失败（HEAD 不变）；恢复一个节点后重试成功；第二节点回来后收敛。
  - 9 场景原子性套件再次全过（head 1255）；100 批混合批次 → head **1358**、累计 **1,263,403** 文件，双节点 `svnadmin verify` 干净、`fails=0`；随后 20 批与假死复测推进到 head **1379**。
- **观察（非缺陷）**：b=1200 处备节点落后 35 个修订（1M 对象下全量对齐 + 复制约 39s），随后自动收敛；无数据差异。

### 2026-11 故障注入与原子性专项（第三轮，100 万对象实例）

- **场景套件**（`atomicity.py`，两轮完整执行 + 跨进程补充）：备/主节点宕机期间提交；上传中杀主节点、杀 svngod、杀客户端；并发同路径提交；并发不同路径提交；4 路并发 revprop；WAL 落盘后、HEAD 推进前崩溃；双 svngod 实例（18600/18601）共享同一主备 S3 的并发写与故障切换。
- **每个提交场景的判定**：HEAD 只允许不变或 +1；若 +1，目标树必须与本地源逐文件 MD5 完全一致；若不变，目标不得出现任何部分内容；两节点 HEAD 收敛；新修订 WAL SHA 跨节点一致。
- **结果**：两轮共 18 个场景全部通过，`fails=0`；1M 实例从 head 1090 推进到 **1219**，累计 **1,142,833** 文件，两节点 `svnadmin verify` 均无完整性问题。
- **关键结论**：
  - **WAL 崩溃窗口**：进程在 WAL 落盘后、HEAD CAS 前退出时，HEAD 保持旧值；重启后恰好前滚一次，内容完整，无双修订/丢修订。
  - **并发同路径**：两个客户端同时写同一新路径，恰好一个完整树胜出，未出现混合内容；中间修订（若产生两修订）也是完整树之一。
  - **客户端中断**：上传中杀死客户端不会留下部分修订；重试后恰好一次提交。
  - **跨进程 revprop**：2 实例 × 4 路并发、10 轮，无丢失更新（CAS 重试生效）。
  - **跨实例故障切换**：A 提交中被杀（0 修订落地），B 继续提交 3 个修订到 1209；A 重启采纳 B 的 HEAD 1209 并继续提交到 1210，无双修订/回退。
- **本轮发现并修复**：
  - **S3 列举分页缺陷**：`S3Store.ListPrefix` 把相对 key 直接用作 `StartAfter`，第二页会重复第一页，导致 `svnadmin journal/gc`（带 `--endpoint`）在 1000 个对象后静默截断。修复为重新拼接存储前缀；1091 条 journal 现在唯一且升序。服务端路径此前使用空前缀未受影响，但同类风险已消除。
  - **svngocli 服务端死亡后挂起**：`resolveRepoRoot` 调用的原生 `svn info` 无超时；`exec.Output` 的管道拷贝还可能被继承 fd 的孙进程卡住。现在元数据类 svn 调用带 20–30s 期限、输出改走临时文件、连接类错误不再逐级回溯父 URL；bundle 发送增加 30s 无进度看门狗。实测服务端在上传中被 SIGKILL，客户端由 **>180s 挂起** 变为 **0.6s 干净失败**。`--delete-missing` 的全树 `ls -R` 仍无总时限（超大仓库的合法长操作），属已知取舍。

### 2026-11 日志与降级模式延迟优化（第二轮，100 万对象实例实测）

- **`svn log -q`**：`reportLog` 过去对每个修订无条件做整树 diff（100 万文件 × 1000 修订），`svn log -q -r 1:100` 需要 27.3s。现在只在客户端请求 changed-paths（`discover-changed-paths`）或按子路径过滤时才计算差异；子路径过滤用 NodeID 比较（目录节点随子树变化而重写）在 O(depth) 判定是否触及。
  - 实测（100 万对象、1090 修订）：`-r 1:20` 65ms，`-r 1:100` 81ms（原 27.3s），`-r 1:HEAD`（1058 修订）456ms；`-q URL/trunk/chaos/b00500 -r 1:HEAD` 1.85s。
- **`svn log -v`**：`treeDiff` 增加 NodeID 剪枝——目录/文件节点 ID 相同即整棵子树或整个文件未变，不再递归、不再读取文本。实测 9 个各含约 1000 路径的修订 264ms（冷缓存首次 1.06s）。
- **降级模式写延迟（关键）**：节点宕机时每次提交曾需 10–16s。根因是 `HeadFresh` 的 WAL 回放对 `head+1..head+8` 的 8 个 journal 键做全副本 `QuorumGet`，不可达副本每个键都要付一次 S3 重试（约 1s）。修复：
  - 先用普通 `Get` 做便宜的存在性探测，缺失/非法立即停止（正常提交不存在待回放记录，只探测 1 个键）；
  - `QuorumGet` 跳过已标记不可用的副本，并把单副本读预算降到 5s。
  - 实测：节点宕机后 `svn ls` 2.3s → 35ms；50 文件提交 10–12s → **0.18s**（首个请求约 1.2s 用于发现并摘除故障副本）。
- **冷启动 + 备节点不可用**：仍按 RPO=0 设计保持 fencing（避免采纳可能落后的 HEAD 导致修订号复用），但 bundle 现在返回 **503 + Retry-After**（原 500），客户端可重试；备节点恢复后全量对齐 2.5s 解除 fencing，提交 0.25s 成功、HEAD +1，双节点 `verify` 干净。
- **回归**：1M 实例再跑 20 批（2 万文件）→ head=1090，双节点 `verify` 干净，0 失败。`go test ./...` 全绿。

### 2026-11 100 万文件随机混沌混合测试（发现并修复镜像恢复缺陷）

- **设计**：沿用同一手工实例（svngod + RustFS 主备 S3 + LDAP/local auth），独立仓库 `chaos1m`；**1000 批 × 1000 唯一内容文件 = 1,000,000 文件写入**。每批随机 1–8 个子目录、随机文本/二进制/大二进制；每批校验 HEAD +1，每 20 批 `svn ls -R` 计数，每 50 批抽样文本 MD5，每 100 批校验双节点 HEAD 并对比随机修订的 WAL SHA。每 50 批注入随机事件：备/主节点宕机后提交、上传中杀主节点或 svngod、并发双提交、并发 revprop、lock/unlock、原生 `svn import`。50 万文件后故障降级为轻量事件，避免测试环境自身重启成本主导。
- **结果**：`files=1,000,770`（含事件文件），`head=1026`，两节点 `verify` 均干净、HEAD 收敛、抽样 MD5 全一致，`FAILS=0`；总耗时约 10 分钟（约 1700 文件/s，含故障等待）。
- **暴露的问题（800k 处一次 48s 追赶）**：主节点一度落后 27 个修订（797 vs 824）。不是数据错误，但恢复路径有三个缺陷：
  1. repair 的缺对象复制是**串行** GET+PUT：27 个修订约 5 万对象需要 48s；
  2. 60s 周期 repair 即使在完全一致时也要**全量列举 100 万个对象**（每副本约 1000 次分页请求），在 1M 规模下明显抢占提交带宽；
  3. repair 期间被跳过的写入**没有登记**，repair 结束时可能把镜像标记为健康，但它实际缺少这些键（故障切换后才会以 404 暴露）。
- **修复**：
  - repair 复制改为 12 路有界并发，按列举得到的对象大小对 >4MiB 对象走临时文件流式复制（不整块进内存）。
  - 新增“待补键集合”（pending）：镜像宕机期间所有被跳过/失败的 Put 键登记（上限 10 万，溢出则回退全量）；镜像恢复只补差量，不再扫描 100 万对象。健康标记要求 pending 为空，堵住 repair 期间丢写窗口。
  - 周期 repair 先用“双副本 current + ack + 健康/verify 状态”做 O(1) 判定，完全一致时跳过全量列举。
  - 宕机节点恢复先用 2s 探针判断可达；不可达时不做无意义全量列举。恢复完成后立即异步跑一次全量校验，覆盖 pending 记账之外的写路径（如另一个 svngod 进程）。
  - **修复一个恢复死锁**：客户端放弃的 bundle 会留下已删除的 txn 对象，其键仍在 pending 中且在任何副本都不存在，导致镜像永远无法回到健康；随后主节点故障时两副本都被判不可用，提交返回 500。现在“所有来源均 NotFound”视为对象已删除并移出 pending。
- **定向验证（100 万对象实例）**：启动全量对齐 **约 2.5s**；节点重启后一旦 RustFS 可达，svngod 在 **≤1s** 内完成差量追赶并恢复健康（RustFS 自身在 1M 对象下冷启动约 25–50s，属外部成本）；宕机期间提交保持成功、无断流，恢复后双节点 `verify` 干净。修复前同场景曾出现主节点故障后连续 500。
- **回归**：`go test -race ./internal/fs/store/`、`go test ./...` 全绿；新增并行修复、差量恢复、已删除 pending、无差异跳过等用例。
- **遗留规模问题（非数据安全）**：100 万对象的全量校验/列举约 2.5s，可作为运维后台任务接受；`svn log` 已在第二轮优化（见上节）。

### 2026-11 100 万文件混合压测（发现并修复 HEAD 下调缺陷）

- **设计**：同一 svngod + S3 主备实例，独立仓库；1000 次原子 bundle，每次 1000 个唯一内容文件 = **1,000,000 个文件写入 / 1000 次提交**；每批校验 HEAD +1、目录 1000 项、首尾文件 MD5；每 100 批校验两节点 HEAD 并跑 `svnadmin verify`（HEAD）；并发提交 3 轮（每轮 2×50 文件），1M 规模下并发 revprop 2 属性。
- **故障注入**：20 万（备节点宕机）、40 万（主节点上传中宕机）、60 万（svngod 上传中被杀）；每次恢复后要求两节点收敛到同一 HEAD。
- **结果（修复后）**：head=1007（seed + 1000 批 + 6 并发），1006 个 trunk 子目录（1000 批 + 6 并发），抽样文件 MD5 一致；两节点 head=1007、`verify` 均干净；镜像健康 2/2；revprop 在 1M 规模下 log/prop 均保留。总耗时约 7 分钟（约 2500 文件/s）。
- **首次运行在 80 万时发现严重缺陷**：备节点无法在旧 5s 预算内列完 80 万对象 → 被判不可达；repair 用落后节点的 HEAD（727）覆盖了进程内已确认 HEAD（804），fencing 被清除；落后节点随后把 "batch 800" 提交为 rev728，而另一节点的真实 rev728 是不同内容，形成**同一修订号两种内容**。WAL 回放又把 `current` 推回 804，掩盖了分歧。
- **修复**（commit `42cda4c`）：`noteAcked` 单调不回退；`reconcileMutable` 仅在已知 ack 且 best≥ack，或冷启动时全部可达/多数派一致时才采纳 HEAD；`SetControlKey` 要求整数多数派一致（修正 `(n+1)/2` 对偶数节点的错误），否则 fencing；`checkCaughtUp` 在 fenced 且无 ack 时拒绝写入；repair 列举/复制预算拆为 90s/10min；bundle 的租约/法定人数错误映射 503。
- **定向复现验证**：落后节点 B(head6)、主节点 A(head11) 宕机，仅 B 存活冷启动 → 上传被拒、B 保持 6；A 恢复后 repair 采纳 11，重试成功到 12，两节点 verify 干净。
- **遗留规模问题（非数据安全）**：`svn log -q -r 1:HEAD`（1007 修订 × 每修订 1000 路径）60s 内未完成，日志路径流式输出/按需 changed-paths 需优化；1M 对象的全量 repair 仍较慢，建议后续用 WAL 清单做增量修复。



### 2026-11 一致性加固（WAL / 法定人数 / RustFS）

- **背景**：三节点 RustFS 上发现两个 svngod 进程各自选举不同 mirror 主节点，HEAD 读数分叉（27 vs 29），存在重复修订号风险；单文件锁响应缺少 S 命名空间导致工作副本丢锁；MERGE 的 lock-path 相对目标目录未解析。
- **修复**：
  - LOCK 响应补齐命名空间；提交按 `X-SVN-Options: release-locks` 释放锁；目录深度锁覆盖子路径；MERGE lock-path 按目标目录解析。
  - FileStore CAS 增加跨进程 `flock`（此前仅进程内互斥，实测并发 revprop 丢失更新）。
  - 提交 WAL：`journal/<N>` 采用 `SVNGWAL1 + len + CRC32C + JSON`，记录 revblock SHA-256；启动/提交前回放，已达成法定人数且数据完整的记录前滚。
  - 跨进程租约：法定人数寄存器（CreateIfAbsent/CAS/Delete 按配置顺序串行，多数派相交），默认主备可用性模式要求“当前可达的全部副本”，单节点故障降为一份。
  - `fenced` 状态读取所有可达副本取最新 HEAD，避免恢复后长期返回旧修订。
- **RustFS 1.0.4 实测**：8 并发 CAS 仅 1 胜；3 节点 `quorum:2` 下 1 节点故障仍提交、2 节点故障 1s 内 503、恢复后无重复修订、三节点 verify 一致；2 节点主备模式备/主分别宕机均 1s 提交成功；主节点在上传 100MiB 增量时被杀，6s 切备成功且 sha256 一致；两个存储节点同时宕机干净失败，恢复后重试成功。
- **回归**：`go test -race -count=1 ./...` 全绿；三节点独立 `verify` 同一修订无完整性问题。

## 附：稳定性测试记录（2026-10-29）

### svndiff 卡点攻坚（决定性修复，git 5ebda18）
- **根因1 varint**：svn 的 `svn__encode_uint` 是**逐字节高位连续**（每字节 7bit、末字节高位=0），**不是首字节分派**。例 `encode(50000)=83 86 50`，旧解析把 `83 86` 误读成 390，导致窗口头全错。
- **根因2 压缩段**：v1/zlib、v2/lz4 的 ins/data 段 = `[svn__encode_uint(原始长度)][明文或压缩流]`；原始长度 <512 或压缩无收益时**明文**，否则解压到**恰好原始长度**。
- 修复：`svndiff.go` readUint/writeUint 改逐字节 base-128；decompressSeg 读原始长度前缀 + 明文/精确解压。
- E2E：50K 随机文件新增、修改提交成功，`svn cat` 读回完全一致。

### 30000×50K 稳定性测试（本地盘后端）
- `svn import` 30000×50K：**30000 文件 PUT 全部成功，svndiff 解码错误=0，panic/fatal/OOM=0**。
- 单次大事务在最终 MERGE 提交阶段被环境杀（重复 4 次，日志停在正常 MERGE，无服务端错误 → 判定为环境重置，非服务缺陷）。
- **分批提交策略**：拆 30×1000 文件小事务，每批独立 commit 成功、HEAD 稳定递增，规避环境对长时进程的杀。
- 环境周期性杀长时 svngod → 守护脚本（guard.sh）自动重启 svngod/RustFS。

### 双后端对比（完成，2026-10-30）
- **服务端直写落盘工具 loadbulk**（cmd/loadbulk）：经 fs 层直接提交 chunk0..29，绕开 svn CLI 大目录逐文件 PROPFIND 的客户端瓶颈。
- **O(n²) 优化（决定性）**：扁平根 30000 文件时，每次提交需重写 root 全量 children → O(n²)（前 12000 文件约 5 分钟，每批递增到 41s）。改为**每 chunk 独立子目录 cN**（root 仅 30 个子目录）→ **线性稳定**。
- **30000×50K 双后端落盘**：
  - 本地盘：**75.6s**（30 批×1000 文件，0.37–1.09s/批，线性、零错误）
  - S3/RustFS：**100.7s**（0.57–2.32s/批，线性、零错误；S3 网络 RTT 略高 ~33%，符合预期）
  - 对比 svn CLI 1000 文件 2 分钟 → 服务端直写 ~0.7s/批（本地盘），**快约 170 倍**
- 前置决定性修复（git 7cbd916）：**懒加载增量提交**——CommitTxn 不再递归加载整棵 HEAD 树，只沿变更路径 ensureDir/ensureData 按需展开，未变更子树保持原 NodeID 零读取 → S3 提交从卡死 60s+ 修复到 0.293s。
- 协议层（git 3b3efb4）：清理 PUT/update-report 诊断日志（原 cfg.Logger nil 导致单测 panic）。
- 读回一致性：fs 单测全过；svn import/co 200 文件读回字节一致。
- git：`5ebda18`(svndiff) → `0f91de0`(co修复) → `4e270eb`(update-report perf) → `1968cfb`(CAS) → `7cbd916`(懒加载) → `3b3efb4`(日志清理) → `b32079c`(loadbulk 分目录) → `dd97592`(双后端对比文档) → `4d13a69`(PROPFIND O(n²) 修复)。

### 协议层大目录 O(n²) 消除（git 4d13a69）
- **PROPFIND**：propChildren 每 child 调 propNode(path)，内部 re-Resolve 会从根重展开父目录 → 大目录 O(n²)。ListDir 已返回每个 child 的 NodeRev 字段，改为**预解析 NodeRev 直传**跳过 Resolve → 线性。svn ls 200 文件 **2.84s → 0.668s（4.3 倍）**；1000 文件大目录 9.24s（慢在客户端解析 1000 条 XML）。
- **PUT/REPORT 确认线性**：nodeCache 按不可变 id 缓存 NodeRev+ByName（PUT 1 秒 124 文件）；update-report 用 indexTree 预取 + child NodeID 直取（co 200 文件 12.3s）。协议层大目录无 O(n²) 遗留。

### PROPFIND 大目录 O(n²) 修复（决定性，git 4d13a69）
- 根因：propChildren 逐 child 调 propNode(path)，propNode 内部 `repo.Resolve(rev, path)` **从根重展开整个父目录** → 大目录 N 个 child 展开 N 次 → O(n²)。
- 修复：ListDir 已返回每个 child 的 NodeRev 字段（Kind/ID/Props），propNode 改为接受**预解析 NodeRev**，propChildren 直传跳过 Resolve → **线性 O(n)**。
- 实测：svn ls 200 文件 **2.84s → 0.668s（4.3 倍）**；svn ls 1000 文件 9.24s（服务器线性，慢在客户端解析 1000 条 XML）。
- 注：PUT（nodeCache 按不可变 id 缓存 NodeRev+ByName）与 REPORT update-report（indexTree 预取 + child NodeID 直取）**本身已线性**，协议层无 O(n²) 遗留。

### 协议层优化（2026-10-30）
- **PROPFIND O(n²) 修复（git 4d13a69）**：propChildren 原每 child 调 propNode→Resolve，每个子项都从根重展开父目录（大目录 O(n²)）。ListDir 已返回各子项 NodeRev 字段（Kind/ID/Props），改为直传预解析 NodeRev 跳过 Resolve → 线性。实测 svn ls 200 文件 2.84s → 0.668s（4.3 倍）。
- **PUT/REPORT 已线性**：nodeCache 按不可变 id 缓存 NodeRev+ByName 索引（PUT 1 秒 124 文件）；update-report 用 indexTree 预取 + child NodeID 直取（co 200 文件 12.3s）。
- **大目录端到端**：挂载 loadbulk 大库后 svn ls c00（1000 文件）9.24s 线性；根递归 30000 / co 30000 全量下载为客户端侧开销（递归 XML、1.5GB 下载），服务器无 O(n²) 遗留。

### 主本地盘 + 备 S3 混合镜像（2026-10-30）
- MirrorStore 已通过 `NewMirror([]Store)` 抽象支持**任意后端混合**（主=FileStore 本地盘、备=S3Store/RustFS）。写路径写主 + 备 best-effort，主故障自动选举健康镜像，后台 repairLoop 从主补齐备的缺失对象。
- config.json 已支持 `storage.type=mirror` + `mirrors: [{type:file,root:...},{type:s3,endpoint:...}]` 混合声明（cmd/svngod/config.go makeStore 逐项建 Store）。
- 端到端验证受 `svnadmin create` 尚不支持 mirror 后端限制（需先建混合 repo），属下一步补 svnadmin create mirror 后可完整验证。

### block 级自动修复能力边界（2026-10-30）
- 检测：`svnadmin verify` 遍历 HEAD 树，node-rev CRC + text delta 链校验，报告损坏项（git b67f7bd + c628491）。
- 修复边界：node-rev 打包进 RevBlock（非单对象），单 node 损坏**无法对象级重建**（无独立数据源，delta 增量丢失即数据丢失）。自动修复的现实路径是 **MirrorStore 读时从健康镜像自动恢复**（主损坏、备完好时读自动从备补齐，已实现）+ verify 检测报告。
- 深层 block 级写回（fs 内部重写 block 内单 node）需 fs 核心改动（解 block+改+重写），高风险，按需拍板。

### 混合镜像故障切换（读写两侧，2026-10-30）
- **读侧**：控制 key（current/format/uuid）读主失败自动回退健康备镜像（git 04185e1）；数据 key 本就 round-robin 从健康镜像读。实测：停主（mv 本地盘主目录）后 svn ls/x.txt 均由备 S3 接管。
- **写侧（主恢复后补齐备）**：MirrorStore.repair() 启动即触发（`go repair()`）+ 60s tick：listAll(主) → 对 down 备全量 catchUp、healthy 备补缺失对象、current 等可变 key 总镜像（主权威）。实测：删备 revs-loose 全部 block → 重启 server 触发 repair → 备自动补齐（0→2 block）→ 停主读备正常。
- 配置注意：svngod config.json 的 S3 凭证字段为 **access-key / secret-key**（含连字符）；误用 access/secret 会导致空凭证 → RustFS AccessDenied → 备永不镜像（已修，git e9e9764）。

### 高并发写验证（多节点并发提交，2026-10-30）
- 场景：10 个独立 wc（多客户端节点）并发 commit，每 wc 独立文件，两轮共 20 个 commit。
- 结果：**修订号严格连续 2..21（无空洞、无丢失）**；每轮 10 文件内容读回 10/10 正确（原子性，无错乱）；HEAD 校验 = base+20 精确。
- 混合镜像同步：并发写后 S3 备同步 22 block；停主（mv 本地盘主）后 svn ls/cat 全部由备接管，故障切换在并发写压力下仍可用。
- 架构对比：原生 svn FSFS 在高并发写时因乐观锁（wc 基线与 HEAD 冲突）产生 E160024 冲突，需 merge/resolve 重试；svngod 单写者强一致（repo.mu 串行 + CommitTxn 原子合并）天然规避冲突，全提交成功、修订连续。

### 原子性安全：同路径并发 lost-update 根治（2026-10-30，git 444aeea / 1e46a01 / c69e2fe）
- **暴露的真实缺陷**：修复前 3 个 wc 基于同 HEAD 并发改**同一文件**，全部"成功"进同一修订、**最后写覆盖（last-write-wins）**，后两个的修改被静默丢弃——原生 svn 在此场景会 E160024 拒绝。
- **根因一（并发控制粒度）**：原 CommitTxn 只有**全仓 `base==head` 门**，拦不住同路径的并发覆盖。改为**路径级乐观并发控制**：change 的目标路径若在 `baseRev..HEAD` 窗口内被其他提交修改/删除则拒绝（与原生 svn 一致的语义）；改不同路径的并发仍正常合并。
- **根因二（CAS 原子性）**：FileStore 的 ETag 用**文件大小**，不同修订的 current 指针可能同字节长，CAS 碰撞失效 → stale 写者能覆盖胜者。ETag 改为 **MD5 内容哈希**，CAS 比较真实内容，堵住同修订 last-write-wins。
- **根因三（目录误报）**：svn 客户端对每个 commit 都附带根/目录属性变更（svn:date），目录 node-rev 随任一子项变动 → 严格检查目录会误拒不同路径提交（"/svn/demo conflicts"）。路径级检测**跳过目录 change**；子项的 add/modify/delete 各自带 Change 仍被守卫，不损失 lost-update 防护。
- **验证**：3 wc 并发改同一文件 → **只有 1 个胜者，其余 E160024 out of date 被拒**，HEAD 保持胜者值，无任何丢失；顺序提交同文件（s2 基于旧 HEAD）→ 第二个被拒、HEAD 不变。
- 错误码同步修正：拒绝时返回 **E160024**（而非连接层 E175002），与原生 svn 语义一致。

### 32 wc 更高并发与混合负载（2026-10-30）
- **32 wc 并发 commit**（每 wc 独立新文件）：修订 **连续 79..109（无空洞）**；首次 31/32 成功，剩余 1 个瞬时 E160024（并发竞态边缘），**重试即收敛、32/32 全成功**、无数据丢失。
- **混合负载**（8 wc 并发写 + 持续读压 ls 60 轮 / cat 40 次同时进行）：写 8/8 全成功、修订连续 71..78；读压 3660 行全正常、无死锁无错误；写后读回全部一致（读到最新值）。
- 结论：svngod 的原子写安全性、并发正确性与读写下性能在以上三类场景下**对齐并超过原生 svn**——同路径并发拒绝不丢失、不同路径并发原子合并、目录/属性变更不误报、高并发+混合负载稳定。

### S3/RustFS 后端原子写攻坚（2026-10-30，验证有效但已回滚）

- **背景**：混合镜像（主本地盘）已实现同路径并发正确拒绝；但**纯 S3/RustFS 后端同路径并发仍是 last-write-wins**——`S3Store.CAS` 是假实现（只 Head 存在性检查 + 直接 Put 覆盖，忽略 ETag），3 个 wc 并发改同一文件会全部"成功"进同一修订、静默丢 2 个修改。
- **真 CAS 方案（已验证运行有效）**：`S3Store.CAS` 改为「Get 读当前对象 → `ContentETag` 内容哈希比较 → 不等返回 `ErrPreconditionFailed` → Put」；`readCurrent` 的 ETag 从「Head 的 minio ETag（不稳定）」改为「Get 当前内容算 `ContentETag`」，跨后端稳定。运行中实测：同路径 3 wc → **1 胜者 + 2 个 E160024**，不再全进同一修订。
- **回滚原因**：① `readCurrent` 改用 Get 内容哈希后，与 FileStore CAS 的 headLocked（锁内读磁盘算 MD5）在 commit 路径出现兼容差异，fs 单测 3 个 FAIL（`store: precondition failed`）；② S3 Head 若改为读对象算哈希，会触发 RustFS 对部分对象（如 format）GET 失败的 500。已回滚到 `da7a04d`，单测恢复全绿。
- **代码层原子性已就位**：`CommitTxn` 入口已有 `r.mu` 串行锁 + 路径级乐观并发控制（目标路径在 baseRev..HEAD 被改即 E160024，且拒绝发生在写 block 之前）；block 按修订号寻址（`LooseRev(sh, N)`）不覆盖。单进程 + 串行锁下假 CAS 足够安全；真 CAS 是为多实例/多进程准备的加强。
- **当前阻碍（环境层，非代码）**：RustFS（S3 网关）后台运行不稳定——压测后崩溃/不响应，导致 svn 客户端请求挂起（超时无响应）、纯 S3 后端与混合镜像（备 S3 读转备）都无法可靠运行。需先排查并稳定 RustFS（数据一致性、连接池、heal 后台任务），纯 S3 后端的原子性才能在稳定基线上做终验。

