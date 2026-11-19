# CHANGELOG

本文件记录 svngod 的变更，重点是**会影响已有部署与运维脚本**的部分。
写在这里的判断标准是：升级后可能出问题的地方，而不是每个提交。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循语义化版本。

---

## [Unreleased] — 2026-11 硬化批次

这一批没有新功能，全部围绕两条主线：**操作原子性**与**数据安全性**。
自 `e29f84a` 起的一批提交。**升级前请完整阅读"升级注意事项"一节**——
其中几项会改变既有部署的行为（`/_metrics` 认证、认证失败限流、连接数上限；
请求体上限默认足够大，通常无需调整）。

### 新增

- **`--version`**：`svngod` / `svnadmin` / `svngocli` 现在都支持 `--version`，输出
  版本号、commit 与构建时间。此前 `svngod` 和 `svnadmin` 完全没有版本信息，
  `svngocli` 打印的是硬编码的 `0.2.0`（从未被任何构建流程更新）。
  配套 `scripts/build.sh` 用 `-ldflags -X` 注入版本，日期取 **commit 时间**而非
  构建时间——同一 commit 重建应得到相同日期。
- **连接数上限** `-max-conns`（默认 512，`0` = 不限）：在 TCP 层限制并发连接，
  超限的连接被立即重置（`ECONNRESET`）。
- **认证失败限流** `-auth-failure-burst`（默认 10/分钟每用户名，`-1` = 禁用）与
  `-auth-failure-window`：失败登录返回 `429` + `Retry-After`。
- **`/_metrics` 认证**：新增 `-metrics-token`（或环境变量 `SVNGOD_METRICS_TOKEN`）。
- **请求体总大小上限** `-max-body-bytes`（默认 1 TiB，`0` = 默认，`-1` = 不限）：
  超限返回 `413`。这是全局兜底——各 endpoint 原有的更严格限额不变。
- **LICENSE**（Apache-2.0）与 **NOTICE**（三方依赖许可）。

### 修复

**数据安全类**

- **`format` 未知字段会被静默抹除**：`UnmarshalFormat` 只解析白名单 key，
  `Marshal` 从结构体重建，所以任何「读-改-写」都会丢掉新版本写入的字段，且无报错。
  现在 `Format.Extra` 原样保留并回写。同时 `OpenRepo` 拒绝比本版本更新的格式
  （`ErrFormatTooNew`），`FORMAT=0` 也被拒绝。
- **GC 三处 fail-open**：读不到引用集时曾继续删。现在一律 fail closed——
  读失败 ≠ 无引用。六条删除支路（chunk manifest / dir 表 / activity manifest /
  packed 标记 / segment 索引 / keep set）全部有守护测试。
- **svndiff 四个解码缺陷**（fuzz 发现）：
  - `readUint` 左移无溢出检查，varint 回绕成负值 → 切片下标为负 → **panic**
  - `npos+int(length)` 溢出绕过边界检查 → **panic**
  - `actionTarget` 的 `length` 无上限，前向自拷贝循环可能跑 2^60 次 → **进程挂死**
  - `SEGMENT_FROM` 单独出现时 `Marshal` 会补 `SEGMENTS=1`，改写后仓库**存储模式静默变化**
- **S3 读可能永久挂死**：`internal/fs` 常以 `context.Background()`（无 deadline）调用存储层，
  而网关「发完响应头就卡住 body」时三种既有保护都失效（`ResponseHeaderTimeout` 在头到达时
  即停表、SDK 只重试传输错误、调用方 ctx 无 deadline）。一次卡顿会永久占住 goroutine，
  提交路径上还占着 commit lease。现在 `Get`/`GetTo`/`Head` 都按**读空闲**设上限（30s，
  每次成功读重置），512 MiB 大对象不受影响。
- **quorum 写可能永久挂死**：`quorumMirror` 给每个副本 15s 的 context 后 `wg.Wait()`，
  但单副本超时只在后端**响应取消**时有效。副本卡在不轮询 ctx 的 syscall 里时
  `wg.Wait()` 无上限——提交路径上即 commit lease 永不释放，后续提交全部排队。
  现在聚合等待有界（15s + 3s grace），未 ack 的副本按 hung 处理并强制下次 repair
  枚举对象，返回值仍是 `ErrQuorumUnmet`（没 ack 不是已 ack）。
- **健康检查返回常量，无法反映真实就绪状态**：`/_api/v1/health` 原来无条件返回
  200 `ok`，即使所有仓库 self-check 失败。放在 LB / Kubernetes readiness 后面会把流量
  导向不能服务的实例。现在任一仓库未就绪即返回 503 `not_ready` 并列出仓库名与原因；
  无就绪记录的仓库按未就绪处理；零仓库配置仍返回 ok。
- **镜像临时文件落在 /tmp，清理正则还漏了 4 类前缀**：镜像写要把整个对象缓冲一份
  （上限 64 GiB），`os.CreateTemp("", ...)` 即 `os.TempDir()`——容器里常是 tmpfs（吃
  内存）或小分区（ENOSPC），且要等到有人提交大文件才暴露。新增 `-temp-dir`（启动时设
  `TMPDIR`，一处覆盖全部 7 个调用点，并真实写探针验证可写）。同时补全清理正则：
  `svngod-mirror-fast-`、`svngod-stream-`、`svngod-repair-`、`svngod-recover-` 此前
  **永不回收**（无报错无日志，只是磁盘慢慢满）。
- **镜像修复的两个 join 没有上限**：listing 后的 `wg.Wait()` 与复制后的 `cw.Wait()`
  只受逐项超时约束，join 本身无界；忽略取消的存储会让整轮修复永久挂住，而修复是备机
  恢复的唯一机制。改为有界 join，**超时即放弃本轮**——继续读 `sets[]`/`reachable[]`/
  `missing[]` 会与仍在运行的 worker 竞争，还可能把半观测状态当成完整状态（实测触发 6
  处数据竞争）。
- **Bundle 分块上传可能卡住请求，超时还会谎报成功**：`handleBundleChunks`（16 worker）
  与 `storeChunksParallel`（8 worker）共 7 处裸 `wg.Wait()`，ctx 来自
  `context.Background()`，服务端不设 `ReadTimeout`/`WriteTimeout`。worker 卡在存储里时
  投递循环先阻塞（缓冲满），等待永远到不了。`storeChunksParallel` 有 5 处等待在**错误
  清理路径**上，校验和失败也会挂住。7 处改走 `waitBounded`，投递进 `select`。
  **超时后不再返回 `firstErr`（可能是 nil 而分块未落盘），查询超时返回 503 而非部分
  `missing` 列表——后者会让客户端跳过上传、静默丢数据。**
- **事务变更片段并行读可能卡住提交请求**：`loadChangeFragments` 用
  `fragmentParallelism=32` 并行读后 `wg.Wait()`，而调用方传 `context.Background()`
  且服务端故意不设 `ReadTimeout`/`WriteTimeout`。存储卡住时请求永不返回、goroutine 滞留。
  现在派发与等待共享一个 30s 预算——**只给等待加超时不够**：信号量获取在派发循环里，
  32 个 worker 全卡住时主循环会阻塞在 `sem <-`，后面的超时根本执行不到。
- **依赖漏洞**：`x/net`、`x/crypto`、`x/text`、`x/sys`、`klauspost/compress` 升级到
  已修复版本。其中 `compress/s2` 的越界读（GO-2026-5841）原本就挂在存储层的导入链上。

**可用性类**

- **panic 隔离**：此前全进程无一处 `recover()`。一个请求 panic 会打掉整个进程，
  所有仓库一起停服。现在 `route` 有内联 recover（已提交响应则只记录日志）、
  后台任务用 `goSafe`、后台长循环用 `recoverLoop`（单轮 panic 不影响下一轮）。
- **批次失败不再谎报修订号**：`CommitBatch` 在发布前失败时不再返回修订号。
- **`load` 中断报出停止位置**，而非笼统的"已放弃"。

### 变更

- **`AllOrNothing()`** 供批量导入使用，保证不留修订号空洞。
- **`/_metrics` 移到认证之后**（见升级注意事项）。
- 提交路径 `CommitBatch` 默认仍是逐条应用（服务端并发合并依赖此语义）。

### 内部质量（无外部可见影响）

- CI 新增：`CGO_ENABLED=1 go test -race`、真实 `svn` 客户端回归、fuzz 语料回放 +
  限时搜索、`govulncheck`（只对**符号可达**漏洞判失败）。
- 反射式元测试：新增 `Collect*`/`Compact*` 方法不写 fail-closed 用例即失败。

---

## 升级注意事项

### 1. `/_metrics` 现在需要认证（**配置了认证的部署会被打破**）

指标标签包含每个仓库名（`svngod_repo_ready{repo=...}`、
`repoRecovered`、`scrubIssues`），暴露出去等于公开租户清单与故障状态。
此前它是 `route` 的第一个分支，在认证之前返回——任何能连端口的人都能读到。

**如果你的部署配置了任何认证链**（`-passwd`、LDAP 或按仓库的 auth），
匿名抓取现在会得到 `401`。二选一：

```yaml
# 方案 A（推荐）：专用 token
scrape_configs:
  - job_name: svngod
    metrics_path: /_metrics
    bearer_token: <metrics-token>        # 服务端用 -metrics-token 或 SVNGOD_METRICS_TOKEN
```

```yaml
# 方案 B：复用仓库账号
    basic_auth:
      username: monitor
      password: <password>
```

**完全没配认证的部署行为不变**（仍然匿名可抓）。这是刻意保留的：那种部署本来
就没有可认证的东西，强行拒绝只会打断现有单机安装。

### 2. 认证失败限流（**可能误伤共享出口 IP**）

默认每用户名 10 次/分钟。**来源地址的预算是它的 10 倍（100 次）**，
因为地址是共享的：办公室 NAT、CI runner 池、反向代理都表现为一个地址。

如果同一个出口后面有很多人，可以调高：

```json
{ "auth-failure-burst": 20, "auth-failure-window": "5m" }
```

要完全关闭：`-auth-failure-burst -1`（注意：`0` 不是关闭，是"未设置"→用默认值。
这个区别是有意的：JSON 省略字段会解析成 0，把 0 当关闭会让配置得很仔细的部署
静默丢掉限流）。

**不信任 `X-Forwarded-For`**：除非代理强制覆写，它由客户端控制。如果你的部署在
反向代理后面且需要按真实客户端限流，应在代理层做。

### 3. 连接数上限（**可能拒绝突发连接**）

默认 512 并发连接。超过的连接被立即重置，而不是排队。大量并发客户端的部署
（如 CI 集群集中 checkout）可能需要调高：`-max-conns 2048`。

如果监控告警突然出现 `connection reset by peer`，先查这个值。

### 4. 请求体上限（默认足够大，一般无需调整）

默认 1 TiB，高于 PUT 的 64 GiB 与 bundle 的 1 TiB 流式限额，正常上传不会受影响
（已用真实 `svn` 客户端提交 5 MB 文件验证 sha256 往返一致）。

需要收紧可设小值（如 `-max-body-bytes 104857600` 限制 100 MiB）；要关闭用 `-1`。
超限返回 `413`——如果客户端报这个，说明请求体确实比配置大。

### 5. 格式兼容：**不要降级**

`format` 标记现在会被版本校验拒绝。**新版本写过的仓库不能再用旧二进制打开**
（会得到 `ErrFormatTooNew` 而不是静默以旧规则读取）。这是刻意的——旧版本读新格式
会按自己的规则解释字段，损坏是静默的。

回滚二进制之前，确认仓库格式版本没有前进过。

### 6. `svnadmin load --batch` 的默认值（不是本批次引入，但升级时常被问到）

默认是 1000 而非逐修订（该默认值来自更早的提交，不是这次硬化批次引入的）。
如果你的脚本依赖"每修订一个 commit"（例如中途检查 HEAD 进度），显式传 `--batch 1`。

批量导入的内存由节点 rev-block 决定：>4 MiB 文本转 chunk manifest，
>8 MiB body 落盘。实测 100 万修订峰值 RSS ≈1 GB。

### 7. 仓库不再提交二进制

此前 `svnadmin`（14 MB macOS arm64）被误提交进版本库，已在 `882110b` 移除。
发布产物请用 `scripts/build.sh`（输出到 `bin/`，已在 `.gitignore` 中）。
直接 `go build ./cmd/...` 会在包目录留下二进制，同样被忽略。

---

## 升级检查清单

```bash
# 1. 构建（带版本注入）
scripts/build.sh 1.2.3

# 2. 确认版本
./bin/svngod --version      # 应显示 1.2.3 (commit ..., built ...)

# 3. 升级前备份（dump 是串行的，按它估窗口，不要按 load 估）
./bin/svnadmin dump --verify --output backup.svn <repo>

# 4. 升级后验证仓库
./bin/svnadmin verify --all-revs --texts <repo>   # 退出码 0 = 干净

# 5. 确认 Prometheus 仍能抓取（如果配了认证）
curl -s -o /dev/null -w '%{http_code}\n' \
  -H "Authorization: Bearer $SVNGOD_METRICS_TOKEN" http://<host>:8080/_metrics

# 6. 确认客户端可用
svn info http://<host>:8080/svn/<repo>
```

**回滚注意**：确认没有仓库的 `format` 版本前进过（`svnadmin info <repo>` 的
`format-version`），否则旧二进制会拒绝打开。

---

## 已知边界（不是缺陷，不要"修"）

这些是刻意的设计决定，改动前请先读 `docs/数据安全与原子性审计清单.md`
第 5 节（完整列表以那里为准）：

- `WriteTimeout` 不设置：多 GB 上传/下载是预期场景（审计清单外，见 `main.go` 注释）
- `segmentEntryAt` 每次复制 block/props：不复制会让 `blockCache` 里一个小条目
  持住整段 0.5 MiB 底层数组，verify 2 万修订的仓就 OOM（有过事故）
- `readLease` 的 `count >= 1`：条件删除的兜底
- `CommitBatch` 默认逐条应用：服务端并发合并依赖它
- `deletePrimaryOrAll` 只删主副本：standby 残留无害，镜像数千次删除会打垮写预算
- `dump` 尾部比头部慢约 4 倍：祖先节点天然分散 + 每段都要解码校验，
  不是缓存策略问题（已实测排除 5 种方案）

`AllOrNothing()` 的语义边界另见审计清单 2.2：它承诺"不留修订号空洞"，
**不承诺"绝不发布"**——v1 的部分 WAL 写入会被 peer replay 前滚为连续前缀。