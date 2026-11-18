# kong-takehome-rate-limiter

Take-home assignment **option 1 of 3: distributed cluster rate-limiting middleware** — a C++17 gateway middleware where N replicas share one global quota pool in Redis.

Every traffic decision is an **atomic Lua execution inside Redis**, so no replica can overspend the shared quota at any concurrency level. See [docs/DESIGN.md](docs/DESIGN.md) for architecture, algorithm trade-offs, and production gaps.

## Features

- **Three algorithms**: fixed window (`fixed_window`), sliding window (`sliding_window`, exact ZSET implementation), token bucket (`token_bucket`, burst allowance + steady refill)
- **Three dimensions**: `ip` (first X-Forwarded-For hop), `client_id` (`X-Client-ID`, `X-Api-Key`), `path` (independent counter per endpoint)
- **Standard protocol**: `429 Too Many Requests` + `Retry-After` on limit, `X-RateLimit-Limit/Remaining/Reset` on every judged response
- **Dynamic config push**: admin API writes to Redis; all replicas converge within one poll interval (default 1s)
- **Configurable degradation**: fail-open (allow + `X-RateLimit-Degraded` marker) or fail-closed (503) when Redis is down; fail-open is the default
- **Reverse proxy**: allowed requests are proxied to `--upstream http://host:port`; without it, the gateway answers with a local echo (handy for demos)

## Build & run

Dependencies: `clang++` (C++17), `make`, `brew install cpp-httplib nlohmann-json redis`.

```bash
make                              # produces bin/rate-limiter
scripts/redis.sh start            # project-local Redis (.redis/, no persistence)

# single instance (local echo mode)
./bin/rate-limiter --listen 0.0.0.0:8080

# 3-replica cluster (production shape)
./bin/rate-limiter --listen 0.0.0.0:8081 --upstream http://127.0.0.1:9000
./bin/rate-limiter --listen 0.0.0.0:8082 --upstream http://127.0.0.1:9000
./bin/rate-limiter --listen 0.0.0.0:8083 --upstream http://127.0.0.1:9000
```

Full flags: `./bin/rate-limiter --help`

## Rule configuration

Rules are JSON objects with six fields (`algorithm`/`dimension`/`path_prefix`/`method`/`limit`/`window_ms`):

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

A `--config rules.json` file can also bootstrap rules at startup (written to Redis, hence cluster-wide). Token bucket semantics: `limit` = bucket capacity (burst ceiling), `window_ms` = time to refill `limit` tokens.

## Admin API (read/write on every replica)

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | liveness probe + Redis status |
| GET | `/admin/v1/rules` | current rule set + config version |
| GET/PUT/DELETE | `/admin/v1/rules/{id}` | get/upsert/delete a rule (writes propagate cluster-wide) |
| GET | `/admin/v1/stats` | per-rule allowed/denied/degraded counters |

## Demo & test

```bash
scripts/run_demo.sh          # 3 replicas + upstream: exact global quota / per-client
                             # pools / live reconfiguration convergence
scripts/integration_test.sh  # 39 end-to-end assertions (algorithms x dimensions x
                             # protocol headers x degradation x convergence)
scripts/redis.sh stop        # shut down the local Redis
```

Concurrency exactness: `python3 tests/load.py <limit> <threads> <per_thread> <port...>` — e.g. limit=100, 8 threads x 50 requests across 3 replicas asserts exactly 100 acceptances cluster-wide.
