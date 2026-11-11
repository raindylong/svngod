# 管理 API 与热加载

> 适用版本：2026-11 起。管理 API 只改变**仓库配置**（存储、authz、认证链），不改变监听地址、TLS、上传上限等进程级参数；后者仍需重启。

## 1. 启用

管理 API 使用**独立监听端口**，默认关闭；启用必须提供 Bearer Token。

```json
{
  "addr": "0.0.0.0:8080",
  "base-path": "/svn",
  "admin-addr": "127.0.0.1:18080",
  "admin-token-file": "/etc/svngod/admin.token",
  "admin-allow-cidr": "127.0.0.1/32,10.0.0.0/8",
  "repos": [ /* ... */ ]
}
```

或命令行 / 环境变量（优先级从高到低）：

| 参数 | 说明 |
|---|---|
| `-admin-addr` | 管理端口；空 = 关闭 |
| `-admin-token` / `SVNGOD_ADMIN_TOKEN` / `admin-token-file` | 令牌；推荐 0600 文件或环境变量 |
| `-admin-allow-cidr` | 允许来源 CIDR，默认 `127.0.0.1/32,::1/128` |

- Token 只在启动时读取；**接口返回的配置会脱敏**（`***`），不会回显 token、S3 密钥。
- 管理 API 需要 `--config`：所有变更要写回 JSON 文件；flag 单库模式只读（变更返回 409）。
- 审计日志：`admin action=add-repo repo="assets" result=ok remote=...`。
- 建议把管理端口限制在内网/回环，或由反向代理终止 TLS。

## 2. 接口

认证：`Authorization: Bearer <token>`（也接受 `X-Admin-Token`）。

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/_api/v1/health` | 存活、generation、仓库列表 |
| GET | `/_api/v1/status` | 最近一次 reload 的结果/忽略字段 |
| GET | `/_api/v1/config` | 当前配置（脱敏） |
| GET | `/_api/v1/repos` | 仓库摘要（名称/存储类型/authz/认证类型） |
| GET | `/_api/v1/repos/{name}` | 单个仓库的完整配置（含密钥，需管理员权限） |
| POST | `/_api/v1/repos` | 新增仓库；`?create=1` 自动建库，`?dry_run=1` 只校验 |
| GET | `/_api/v1/repos` | 返回每个仓库的 `ready` / `ready_error`（启动自检与热恢复状态） |
| PUT | `/_api/v1/repos/{name}` | 替换仓库配置 |
| DELETE | `/_api/v1/repos/{name}` | 从配置移除（**不删除数据**） |
| POST | `/_api/v1/reload` | 重读配置文件并热加载 |

所有变更串行执行，返回新的 `generation`。

### 2.1 新增并自动建库

```bash
curl -X POST "http://127.0.0.1:18080/_api/v1/repos?create=1" \
  -H "Authorization: Bearer $SVNGOD_ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "name": "assets",
    "storage": {"type":"s3","endpoint":"10.0.0.11:9000",
                "access-key":"k","secret-key":"s","bucket":"svn","prefix":"repos"},
    "authz": "/etc/svngod/authz-assets",
    "auth": [{"type":"local","passwd":"/etc/svngod/passwd"}]
  }'
# 201 {"generation":2,"name":"assets"}
```

- `create=1`：仓库不存在时按默认参数建库（分片 4096、最大 delta 链 64），已存在返回 409。
- `create=1` 时可附加 `"segments": true`、`"segment-target": 4096`，新库使用 v2 段提交格式（每 4096 修订合并为一段，S3 上对象数从 40 万级降到千级）；对已存在的库无影响。
- 不传 `create`：只加配置，仓库必须已存在，否则 422。
- `dry_run=1`：只做校验，不建库、不落盘、不切换；返回 `{"dry_run":true,"valid":true}`。

### 2.2 替换 / 删除

```bash
curl -X PUT http://127.0.0.1:18080/_api/v1/repos/assets \
  -H "Authorization: Bearer $SVNGOD_ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d @assets.json

curl -X DELETE http://127.0.0.1:18080/_api/v1/repos/assets \
  -H "Authorization: Bearer $SVNGOD_ADMIN_TOKEN"
# {"generation":5,"name":"assets","note":"configuration removed; repository data was not deleted"}
```

替换存储不会迁移数据；删除只改配置并在新请求上返回 404，**仓库对象与数据保留**。

### 2.3 热加载

```bash
curl -X POST http://127.0.0.1:18080/_api/v1/reload \
  -H "Authorization: Bearer $SVNGOD_ADMIN_TOKEN"
# {"changed":["assets"],"generation":4,"ignored_fields":null,"repos":["assets","demo"]}
```

也支持 `kill -HUP <svngod pid>` 触发同一逻辑（配置文件模式）。

## 3. reload 语义

1. **重新读取配置文件**并逐仓构建 store/auth/authz，并 `OpenRepo` 探测仓库存在。
2. **全部成功才切换**：任一仓库失败返回 422，旧 generation 继续服务；坏 JSON 同样不会影响运行中的服务。
3. **按仓库差异重建**：JSON 指纹未变的仓库复用原描述符与 `*fs.Repo` 单例，不重新打开 store、不重置提交锁。
4. **变更/删除的仓库**：从 `repoCache` 失效；在途请求继续使用旧对象，旧 store 延迟 30 秒关闭（停止 mirror repair/健康 goroutine）。
5. **全局字段不热更**：`addr`、`base-path`、`hooks`、`tls-*`、`max-uploads`、`max-user-uploads`、`disable-metrics`、`admin-*` 的差异会列在 `ignored_fields`，需重启。
6. **持久化**：新增/替换/删除会原子改写配置文件（临时文件 + fsync + rename，保留 `.bak`，沿用原文件权限）；reload 本身不写文件。

## 4. 失败与状态

| 场景 | 返回 |
|---|---|
| 未带/错误 token | `401` |
| 来源不在 `admin-allow-cidr` | `403` |
| 名称非法、字段缺失、authz/passwd 不可读 | `422` + 具体错误 |
| 重复仓库名 / `create` 时已存在 | `409` |
| 配置文件不可写 / 无 `--config` | `500` / `409` |
| token 未配置但启用了 admin 端口 | 进程拒绝启动 |

`GET /_api/v1/status` 示例：

```json
{"generation":4,"repos":["assets","demo"],"last_reload":"2026-11-07T12:00:00Z",
 "result":"reload-ok","ignored_fields":["max-uploads"]}
```

## 5. 多实例

- 每个进程有自己的配置文件；API 只作用于**本进程 + 本机文件**。
- 多实例部署时，由配置管理/CI 依次调用各实例的管理端口，或只调用一个实例后同步文件并对其余实例发 `SIGHUP`/`POST /reload`。
- 不提供跨实例的配置广播；避免多个进程同时写同一配置文件。

## 6. 安全清单

- 管理端口默认关闭，启用必须有 token；默认只允许回环来源。
- 使用 0600 的 token 文件或环境变量，不要把 token 提交进仓库。
- 对外只暴露业务端口；管理端口放在内网或经反向代理 + TLS。
- 审计日志保留 `action`、仓库名、来源地址；`GET /config` 已脱敏。
- 删除仓库不会删数据；如需回收存储，另行执行运维清理。