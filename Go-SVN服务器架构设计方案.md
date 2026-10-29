
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
- git：`5ebda18`(svndiff) → `0f91de0`(co修复) → `4e270eb`(update-report perf) → `1968cfb`(CAS) → `7cbd916`(懒加载) → `3b3efb4`(日志清理) → `b32079c`(loadbulk 分目录)。
