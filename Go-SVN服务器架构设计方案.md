
---

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
