# kong-takehome-rate-limiter

考题一：**分布式集群限流插件实战** —— 基于 Redis 全局配额池的多副本限流中间件（C++17）。

N 个网关副本共享同一个配额池：每个请求的流量决策都是 Redis 内的一次 **Lua 原子执行**，任何副本、任何并发度下都不会超发。方案细节、算法取舍与生产缺口见 [docs/DESIGN.md](docs/DESIGN.md)。

## 功能

- **三种限流算法**：固定窗口（`fixed_window`）、滑动窗口（`sliding_window`，ZSET 精确实现）、令牌桶（`token_bucket`，支持突发流量 + 匀速补充）
- **三种控制维度**：`ip`（含 X-Forwarded-For 首跳）/ `client_id`（`X-Client-ID`、`X-Api-Key`）/ `path`（每路径独立计数）
- **标准协议**：超限返回 `429 Too Many Requests` + `Retry-After`，响应携带 `X-RateLimit-Limit/Remaining/Reset`
- **动态配置下发**：管理 API 写入 Redis，所有副本按版本号轮询收敛（默认 1s）
- **降级策略可配**：Redis 故障时 fail-open（放行 + `X-RateLimit-Degraded`）或 fail-closed（503），默认 fail-open
- **反向代理**：可将放行的请求代理到上游 `--upstream http://host:port`；未配置时本地回显（便于演示/联调）

## 构建与运行

依赖：`clang++`（支持 C++17）、`make`、`brew install cpp-httplib nlohmann-json redis`。

```bash
make                              # 产出 bin/rate-limiter
scripts/redis.sh start            # 项目内本地 Redis（.redis/，无持久化）

# 单实例（本地回显模式）
./bin/rate-limiter --listen 0.0.0.0:8080

# 三副本集群（生产形态演示）
./bin/rate-limiter --listen 0.0.0.0:8081 --upstream http://127.0.0.1:9000
./bin/rate-limiter --listen 0.0.0.0:8082 --upstream http://127.0.0.1:9000
./bin/rate-limiter --listen 0.0.0.0:8083 --upstream http://127.0.0.1:9000
```

完整参数：`./bin/rate-limiter --help`

## 规则配置

规则 JSON（`algorithm`/`dimension`/`path_prefix`/`method`/`limit`/`window_ms` 六项）：

```bash
curl -X PUT http://127.0.0.1:8080/admin/v1/rules/api-limit -d '{
  "id": "api-limit",
  "algorithm": "sliding_window",
  "dimension": "client_id",
  "path_prefix": "/api/",
  "method": "*",
  "limit": 100,
  "window_ms": 60000
}'
```

也可用 `--config rules.json` 在启动时以规则文件引导（写入 Redis 后全集群生效）。令牌桶语义：`limit` = 桶容量（突发上限），`window_ms` = 补充 `limit` 个令牌的周期。

## 管理 API（每副本均可读写）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 存活探针 + Redis 状态 |
| GET | `/admin/v1/rules` | 当前规则集与配置版本 |
| GET/PUT/DELETE | `/admin/v1/rules/{id}` | 规则查/建改/删（写即全集群下发） |
| GET | `/admin/v1/stats` | 每规则 allowed/denied/degraded 计数 |

## 演示与测试

```bash
scripts/run_demo.sh          # 3 副本 + 上游：全局精确限流 / 配额隔离 / 动态下发收敛
scripts/integration_test.sh  # 39 项端到端断言（算法×维度×协议×降级×收敛）
scripts/redis.sh stop        # 关闭本地 Redis
```

并发正确性：`python3 tests/load.py <limit> <threads> <per_thread> <port...>` —— 例如 limit=100、8 线程×50 请求轮询 3 副本，断言全集群恰好放行 100 个。
