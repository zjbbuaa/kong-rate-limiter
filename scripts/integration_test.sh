#!/usr/bin/env bash
# End-to-end integration test: single gateway instance against real Redis.
# Covers the MVP closed loop:
#   fixed window / sliding window / token bucket  x  client_id / ip / path dims
#   429 + Retry-After, X-RateLimit-* headers, dynamic config push,
#   fail-open degradation, multi-replica config convergence (2nd instance).
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=bin/rate-limiter
BASE_PORT=18080
ADMIN="$BASE_PORT"   # admin API shares the same port
PASS=0
FAIL=0

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); echo "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL - $1"; }
check(){ # check <desc> <actual> <expected>
  if [ "$2" = "$3" ]; then ok "$1 (got '$2')"; else bad "$1 (want '$3', got '$2')"; fi
}
check_true(){ # check_true <desc> <actual-nonempty-true>
  if [ "$2" = "1" ] || [ "$2" = "true" ]; then ok "$1"; else bad "$1 (got '$2')"; fi
}

cleanup() {
  [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null
  [ -n "${SRV2_PID:-}" ] && kill "$SRV2_PID" 2>/dev/null
}
trap cleanup EXIT

scripts/redis.sh start >/dev/null
scripts/redis.sh cli flushall >/dev/null   # pristine quota/config state

say "build"
make "$BIN" >/dev/null || exit 1
ok "make"

say "start gateway on :$BASE_PORT (local echo mode, fail-open)"
"$BIN" --listen 127.0.0.1:$BASE_PORT --poll-ms 200 >/tmp/rl-test.log 2>&1 &
SRV_PID=$!
sleep 0.6

# ---------------------------------------------------------------- health
say "health"
check "GET /healthz -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$ADMIN/healthz)" 200

# ---------------------------------------------------------------- no rule => pass-through
say "pass-through (no matching rule)"
check "GET /anything -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BASE_PORT/anything)" 200

# ------------------------------------------------- fixed window, client_id dim
say "fixed_window / client_id: limit 3 per 10s"
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/fw-cid" -d '{
  "id":"fw-cid","algorithm":"fixed_window","dimension":"client_id",
  "path_prefix":"/api/","method":"*","limit":3,"window_ms":10000}' >/dev/null
sleep 0.5
H=(-H 'X-Client-ID: alice')
check "req1 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${H[@]}" http://127.0.0.1:$BASE_PORT/api/a)" 200
check "req2 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${H[@]}" http://127.0.0.1:$BASE_PORT/api/b)" 200
OUT3=$(curl -s -D - -o /dev/null "${H[@]}" http://127.0.0.1:$BASE_PORT/api/c)
check "req3 -> 200" "$(printf '%s' "$OUT3" | head -1 | grep -c 200)" 1
check "req3 X-RateLimit-Remaining: 0" \
  "$(printf '%s' "$OUT3" | awk 'tolower($1)=="x-ratelimit-remaining:"{gsub(/\r/,"");print $2}')" 0
OUT=$(curl -s -D - -o /dev/null "${H[@]}" http://127.0.0.1:$BASE_PORT/api/d)
check "req4 -> 429" "$(printf '%s' "$OUT" | head -1 | grep -c '429')" 1
check "Retry-After present" "$(printf '%s' "$OUT" | grep -ci 'retry-after:')" 1
check "429 X-RateLimit-Remaining: 0" \
  "$(printf '%s' "$OUT" | awk 'tolower($1)=="x-ratelimit-remaining:"{gsub(/\r/,"");print $2}')" 0
check "429 body is JSON error" \
  "$(curl -s "${H[@]}" http://127.0.0.1:$BASE_PORT/api/d | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["error"])')" rate_limit_exceeded
check "other client (bob) unaffected -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: bob' http://127.0.0.1:$BASE_PORT/api/a)" 200
check "no client header (anonymous) unaffected -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BASE_PORT/api/a)" 200

# ------------------------------------------------- ip dimension (X-Forwarded-For)
say "fixed_window / ip (X-Forwarded-For first hop): limit 2 per 10s"
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/fw-ip" -d '{
  "id":"fw-ip","algorithm":"fixed_window","dimension":"ip",
  "path_prefix":"/ip/","method":"*","limit":2,"window_ms":10000}' >/dev/null
sleep 0.5
F=(-H 'X-Forwarded-For: 203.0.113.7, 70.41.3.18')
check "req1 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${F[@]}" http://127.0.0.1:$BASE_PORT/ip/x)" 200
check "req2 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${F[@]}" http://127.0.0.1:$BASE_PORT/ip/x)" 200
check "req3 -> 429 (same first hop)" "$(curl -s -o /dev/null -w '%{http_code}' "${F[@]}" http://127.0.0.1:$BASE_PORT/ip/x)" 429
check "different IP -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Forwarded-For: 198.51.100.2' http://127.0.0.1:$BASE_PORT/ip/x)" 200

# ------------------------------------------------- path dimension
say "fixed_window / path (per-endpoint limiting): limit 1 per 10s"
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/fw-path" -d '{
  "id":"fw-path","algorithm":"fixed_window","dimension":"path",
  "path_prefix":"/p/","method":"*","limit":1,"window_ms":10000}' >/dev/null
sleep 0.5
check "GET /p/one -> 200" "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BASE_PORT/p/one)" 200
check "GET /p/one again -> 429" "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BASE_PORT/p/one)" 429
check "GET /p/two -> 200 (separate counter)" "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BASE_PORT/p/two)" 200

# ------------------------------------------------- sliding window
say "sliding_window / client_id: limit 3 per 5s (no boundary burst)"
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/sw-cid" -d '{
  "id":"sw-cid","algorithm":"sliding_window","dimension":"client_id",
  "path_prefix":"/sw/","method":"*","limit":3,"window_ms":5000}' >/dev/null
sleep 0.5
S=(-H 'X-Client-ID: carol')
for i in 1 2 3; do
  check "req$i -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${S[@]}" http://127.0.0.1:$BASE_PORT/sw/)" 200
done
OUT=$(curl -s -D - -o /dev/null "${S[@]}" http://127.0.0.1:$BASE_PORT/sw/)
check "req4 -> 429" "$(printf '%s' "$OUT" | head -1 | grep -c 429)" 1
check_true "Retry-After > 0" \
  "$(printf '%s' "$OUT" | awk 'tolower($1)=="retry-after:"{gsub(/\r/,"");print ($2>0)}')"

# ------------------------------------------------- token bucket
say "token_bucket / client_id: burst 2, refill 2 per 4s"
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/tb-cid" -d '{
  "id":"tb-cid","algorithm":"token_bucket","dimension":"client_id",
  "path_prefix":"/tb/","method":"*","limit":2,"window_ms":4000}' >/dev/null
sleep 0.5
T=(-H 'X-Client-ID: dave')
check "burst req1 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${T[@]}" http://127.0.0.1:$BASE_PORT/tb/)" 200
check "burst req2 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' "${T[@]}" http://127.0.0.1:$BASE_PORT/tb/)" 200
check "burst req3 -> 429" "$(curl -s -o /dev/null -w '%{http_code}' "${T[@]}" http://127.0.0.1:$BASE_PORT/tb/)" 429
check "after refill (4.2s) -> 200" \
  "$(sleep 4.2; curl -s -o /dev/null -w '%{http_code}' "${T[@]}" http://127.0.0.1:$BASE_PORT/tb/)" 200

# ------------------------------------------------- dynamic config push + convergence
say "dynamic config push converges to a 2nd replica"
"$BIN" --listen 127.0.0.1:18081 --poll-ms 200 >/tmp/rl-test2.log 2>&1 &
SRV2_PID=$!
sleep 0.6
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/dyn" -d '{
  "id":"dyn","algorithm":"fixed_window","dimension":"client_id",
  "path_prefix":"/dyn/","method":"*","limit":1,"window_ms":10000}' >/dev/null
sleep 0.8   # > poll interval: replica 2 must have picked it up
check "replica1 -> 200" "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: erin' http://127.0.0.1:$BASE_PORT/dyn/)" 200
check "replica1 again -> 429" "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: erin' http://127.0.0.1:$BASE_PORT/dyn/)" 429
check "replica2 (config pulled from Redis) -> 429" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: erin' http://127.0.0.1:18081/dyn/)" 429
check "replica2 other client -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: frank' http://127.0.0.1:18081/dyn/)" 200
curl -s -X DELETE "http://127.0.0.1:$ADMIN/admin/v1/rules/dyn" >/dev/null
sleep 0.8
check "rule deleted on replica2 -> 200" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: erin' http://127.0.0.1:18081/dyn/)" 200

# ------------------------------------------------- fail-open on Redis outage
say "fail-open while Redis is down"
scripts/redis.sh stop >/dev/null
sleep 0.3
OUT=$(curl -s -D - -o /dev/null -H 'X-Client-ID: alice' http://127.0.0.1:$BASE_PORT/api/a)
check "traffic still allowed (200)" "$(printf '%s' "$OUT" | head -1 | grep -c 200)" 1
check "X-RateLimit-Degraded: 1" "$(printf '%s' "$OUT" | grep -ci 'x-ratelimit-degraded: 1')" 1
scripts/redis.sh start >/dev/null
scripts/redis.sh cli flushall >/dev/null

# ------------------------------------------------- fail-closed mode
say "fail-closed mode (--fail-closed)"
kill $SRV_PID 2>/dev/null; wait $SRV_PID 2>/dev/null
"$BIN" --listen 127.0.0.1:$BASE_PORT --fail-closed --poll-ms 200 >/tmp/rl-test.log 2>&1 &
SRV_PID=$!
sleep 0.6
# flushall wiped the rule set: re-create a rule so the protected path is
# actually guarded when Redis goes down.
curl -s -X PUT "http://127.0.0.1:$ADMIN/admin/v1/rules/fw-cid" -d '{
  "id":"fw-cid","algorithm":"fixed_window","dimension":"client_id",
  "path_prefix":"/api/","method":"*","limit":3,"window_ms":10000}' >/dev/null
sleep 0.3
scripts/redis.sh stop >/dev/null
sleep 0.3
check "503 while Redis down" \
  "$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Client-ID: alice' http://127.0.0.1:$BASE_PORT/api/a)" 503
scripts/redis.sh start >/dev/null
scripts/redis.sh cli flushall >/dev/null

# ------------------------------------------------- validation
say "admin validation"
check "bad algorithm -> 400" \
  "$(curl -s -o /dev/null -w '%{http_code}' -X PUT http://127.0.0.1:$ADMIN/admin/v1/rules/x -d '{"id":"x","algorithm":"nope","dimension":"ip","path_prefix":"/","method":"*","limit":1,"window_ms":1000}')" 400
check "id mismatch -> 400" \
  "$(curl -s -o /dev/null -w '%{http_code}' -X PUT http://127.0.0.1:$ADMIN/admin/v1/rules/y -d '{"id":"z","algorithm":"fixed_window","dimension":"ip","path_prefix":"/","method":"*","limit":1,"window_ms":1000}')" 400

echo
echo "==============================="
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
