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
- 真实 OpenLDAP（direct-bind + search+bind）认证 + authz 授权。
- 多库独立 authz / 独立认证（本地表 + LDAP 混合）E2E。
- 主备 S3 镜像：双写 / 读负载均衡 / 故障降级 / 恢复自动补齐 E2E。
- 1000 万修订级压测（自动分片打包）。

## 设计文档

- `docs/Go-SVN服务器架构设计方案.md`（v1.1，含 §9.4 多库、§9.5 主备镜像）
- `docs/Go-SVN存储引擎实现细则.md`
