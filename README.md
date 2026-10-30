# svngod — Subversion 服务器（Go 实现）

用 Go 重写的 SVN 服务器：客户端走原生 HTTP/HTTPS（WebDAV/DeltaV，ra_serf HTTPv1），认证支持 OpenLDAP + 本地密码表，authz 组定义在本地文件，版本文件存储在 S3 兼容对象存储（默认 RustFS）或本地目录，支持单库主备 S3 镜像与多库/多后端。

## 特性

- **协议层**：WebDAV/DeltaV HTTPv1，覆盖 `OPTIONS/PROPFIND/GET/REPORT/MKACTIVITY/PROPPATCH/CHECKOUT/PUT/DELETE/MKCOL/MERGE/COPY/MOVE/LOCK/UNLOCK` 与私有 `!svn/*` URL。svn CLI 常规命令（checkout/import/commit/update/status/log/ls/info/blame/diff）端到端打通。
- **认证**：OpenLDAP（search+bind / direct-bind 两种模式）+ 本地密码表（`{SHA}` / `{CRYPT}` SHA-512crypt），按序首成功；真实 OpenLDAP 集成验证。
- **授权**：SVN authz 文件（组传递闭包、`~` 否定、max-of-matches），每库独立 authz。
- **存储**：svndiff v0 编解码、FSFS 式分片（SHARD_SIZE 默认 4096）、自动打包、LRU 缓存；后端 `file`（本地）/ `s3`（RustFS/MinIO/AWS/OSS/COS）/ `mirror`（主备镜像）。
- **规模**：单仓 1000 万修订级（分片+自动打包，loose 对象数 O(shards)）；实测 10 万修订 avg 0.44ms/rev 稳定不退化。
- **多租户**：`--config` 单进程多库，每库独立存储/认证/授权；`mirror` 提供主备 S3 负载均衡 + 故障降级 + 恢复自动补齐。

## 快速开始

```bash
go build -o svngod ./cmd/svngod
go build -o svnadmin ./cmd/svnadmin

# 本地单库（flag 模式）
svnadmin create /srv/svn/demo
svngod --addr 127.0.0.1:8080 --base-path /svn --repo-root /srv/svn \
  --passwd /etc/svngod/passwd --authz /etc/svngod/authz &

# 多库 / 多后端 / 主备镜像（JSON 配置）
svngod --config svngod.json
```

配置示例见 `docs/Go-SVN服务器架构设计方案.md` §9.4（多库）、§9.5（主备 S3 镜像）。

### 启动（S3 后端，当前环境示例）

需要先有一个 S3 兼容对象存储。工程内置的本地 RustFS 用法如下：

```bash
# 1. 启动本地 RustFS（S3 兼容，端口 9000；凭证必须经 env 传入）
RUSTFS_ACCESS_KEY=k RUSTFS_SECRET_KEY=s \
  nohup /home/user/svngod/rustfs-bin server /home/user/svngod/rustfs-data \
  > /tmp/rustfs.log 2>&1 &

# 2. 启动 svngod（多库 JSON 配置，端口 19998）
/home/user/svngod/bin/svngod --config /home/user/svngod/stab/s3-cas.json
```

`s3-cas.json` 要点：`endpoint=127.0.0.1:9000`、`ak=k`、`sk=s`、`bucket=svn`、`prefix=repos`，认证用本地密码表 `stab/s3/passwd`（账号 `alice:secret`）。

svn 客户端固定使用自备份二进制（1.14.1），并依赖系统 libsvn：

```bash
SVN=/home/user/svngod/tools/svn
export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
```

## 用 svn CLI 简单测试

```bash
SVN=/home/user/svngod/tools/svn
URL=http://127.0.0.1:19998/svn/demo-cas
A="--username alice --password secret --non-interactive --no-auth-cache"

# checkout / add / commit
$SVN co $URL /tmp/w $A
echo hello > /tmp/w/f.txt
$SVN add /tmp/w/f.txt
$SVN commit -m "init" /tmp/w/f.txt $A

# update（增量与 no-op 均验证 update-report 读路径）
$SVN update /tmp/w $A

# 读路径
$SVN status /tmp/w $A
$SVN log $URL $A
$SVN diff -r 1:2 $URL $A

# 并发提交 —— 路径级 CAS / 交错提交（对齐标准 SVN）
# 不同路径（独立文件）：各 WC 并发提交全部成功，成为连续修订（交错）
for i in 1 2 3; do
  (cd /tmp/w$i && $SVN commit -m "indep$i" indep$i.txt $A &)
done
# 同路径：1 胜 + N-1 个 E160024（冲突拒绝，不静默覆盖）
for i in 1 2 3; do
  ($SVN commit -m "c$i" /tmp/w/f.txt $A &)
done
wait
```

## 工程结构

```
internal/
  fs/        存储引擎：CommitTxn / Resolve / GetText / ListDir / PackShard
    store/   store 抽象：File / Memory / S3 / Mirror + keys + svndiff
  server/    WebDAV/DeltaV 协议层（dav / commit / report）
  auth/      认证：LDAP + 本地密码表 + 链
  authz/     授权：authz 文件解析与路径权限
cmd/
  svngod/    服务器 CLI（flag 单库 + --config 多库/镜像）
  svnadmin/  create / dump / load 迁移工具
  perf/      修订生成压测（自动分片打包验证）
```

## 验收状态

- 真实 svn CLI 1.14.1（ra_serf HTTPv1）端到端：co / import / commit / update / status / log / ls / info / blame / diff。
- **P0 S3 真 CAS 原子写**：Store.CAS 按内容哈希条件写（Get→ContentETag 比对→不等 `ErrPreconditionFailed`→Put），同路径并发提交 1 胜 + N-1 失败；`TestConcurrentSamePathCommit`（FileStore + 真实 RustFS）`-count=50`、`go test -race ./...` 全绿。
- **路径级 CAS / 交错提交**：per-repo 单例复用 + CommitTxn 路径级 out-of-date 检查，**不同路径并发提交全部成功（成为连续修订）**，同路径并发保持 1 胜 + N-1×E160024（对齐标准 SVN）。
- **三后端并发复核（端到端）**：S3/RustFS 并发独立文件 5/5 成功（HEAD 65→70）+ 同路径 1 胜 2 败（→71）；**本地盘 file** 后端 5/5 成功（11→16）；**Mirror 镜像**（自建 file+file 主备）5/5 成功（0→5，主备双写同步）。同路径冲突败者 `svn update` 走**标准文本冲突 C** 语义（生成 `.mine/.r73/.r74`），`svn resolve --accept theirs-full` 后提交 no-op（HEAD 75）。
- **P1 update-report 读路径回归**：对 WC 已存在节点发 `open-file`+`fetch-file`（而非 `add-file`，避免客户端 `add_file()` 把已版本化文件判为 obstructing working copy），`<S:entry start-empty>` 区分 co/update，根/目录 `rev=baseRev`，`V:md5-checksum`。`svn co` / 增量 update（文件内容正确更新）/ no-op update 全部正常（E155024 根治，与 mod_dav_svn 行为一致）。
- **E155017 根治（update-report no-op）**：客户端 base 之后未变文件不再发 `fetch-file`（`c.ID.Rev <= baseRev` 跳过），败者 update 不再校验失败，正确进入标准文本冲突路径。
- 真实 OpenLDAP（direct-bind + search+bind）认证 + authz 授权。
- 多库独立 authz / 独立认证（本地表 + LDAP 混合）E2E。
- 主备 S3 镜像：双写 / 读负载均衡 / 故障降级 / 恢复自动补齐 E2E。
- 1000 万修订级压测（自动分片打包）。

## 设计文档

- `docs/Go-SVN服务器架构设计方案.md`（v1.1，含 §9.4 多库、§9.5 主备镜像）
- `docs/Go-SVN存储引擎实现细则.md`
