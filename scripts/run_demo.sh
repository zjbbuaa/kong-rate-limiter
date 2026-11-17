#!/usr/bin/env bash
# The showcase: 3 gateway replicas + 1 upstream + shared Redis.
#   1. a global limit is enforced exactly across all replicas
#   2. per-client quota pools are isolated
#   3. a live rule change (PUT to ONE replica) converges to all replicas
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=bin/rate-limiter
UPSTREAM_PORT=18090
PORTS=(8081 8082 8083)
CLIENT="demo-$(date +%s)"

step() { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  kill ${UP_PID:-} 2>/dev/null || true
}
trap cleanup EXIT

scripts/redis.sh start >/dev/null
scripts/redis.sh cli flushall >/dev/null
make "$BIN" >/dev/null

step "start upstream on :$UPSTREAM_PORT"
python3 tests/upstream.py "$UPSTREAM_PORT" &
UP_PID=$!
sleep 0.4

step "start 3 gateway replicas (proxy -> upstream, shared Redis)"
for p in "${PORTS[@]}"; do
  "$BIN" --listen 127.0.0.1:$p --upstream http://127.0.0.1:$UPSTREAM_PORT \
      --poll-ms 500 >/tmp/rl-demo-$p.log 2>&1 &
  PIDS+=($!)
  info "replica :$p up (pid $!)"
done
sleep 0.6

step "create rule on replica :${PORTS[0]}: client_id 'demo' = 6 req/min"
curl -s -X PUT "http://127.0.0.1:${PORTS[0]}/admin/v1/rules/demo" -d '{
  "id":"demo","algorithm":"fixed_window","dimension":"client_id",
  "path_prefix":"/","method":"*","limit":6,"window_ms":60000}' | python3 -m json.tool
sleep 0.8   # let all replicas poll it in

step "fire 18 requests round-robin across the 3 replicas (same client)"
ok=0; denied=0
for i in $(seq 1 18); do
  p=${PORTS[$(( (i-1) % 3 ))]}
  code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Client-ID: $CLIENT" "http://127.0.0.1:$p/order")
  if [ "$code" = 200 ]; then ok=$((ok+1)); else denied=$((denied+1)); fi
done
info "accepted=$ok denied=$denied  <- exactly 6 accepted, no matter which replica served the request"
[ "$ok" = 6 ] || { echo "DEMO FAILED: expected 6 accepted"; exit 1; }

step "a different client gets its own quota pool"
ok=0
for i in 1 2 3 4 5 6 7; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Client-ID: other-$CLIENT" "http://127.0.0.1:${PORTS[$((i%3))]}/order")
  [ "$code" = 200 ] && ok=$((ok+1))
done
info "other client accepted=$ok/7 (its own 6 allowed + 1 denied)"
[ "$ok" = 6 ] || { echo "DEMO FAILED: expected 6 for the other client"; exit 1; }

step "live reconfiguration: raise limit 6 -> 8 via :${PORTS[1]} admin API"
curl -s -X PUT "http://127.0.0.1:${PORTS[1]}/admin/v1/rules/demo" -d '{
  "id":"demo","algorithm":"fixed_window","dimension":"client_id",
  "path_prefix":"/","method":"*","limit":8,"window_ms":60000}' >/dev/null
info "waiting for poll convergence (0.8s)..."
sleep 0.8
step "fresh client under the new limit: 9 requests round-robin"
info "(denied requests also advance the shared window counter, so the old"
info " client's window stays exhausted -- this uses a fresh client id)"
ok=0
for i in $(seq 1 9); do
  p=${PORTS[$(( (i-1) % 3 ))]}
  code=$(curl -s -o /dev/null -w '%{http_code}' -H "X-Client-ID: raised-$CLIENT" "http://127.0.0.1:$p/order")
  [ "$code" = 200 ] && ok=$((ok+1))
done
info "accepted=$ok/9  <- 8 proves ALL replicas now enforce limit=8"
info "   (a replica still on limit=6 would cap the shared counter at 6)"
[ "$ok" = 8 ] || { echo "DEMO FAILED: expected 8 accepted after limit raise"; exit 1; }

step "per-replica stats"
for p in "${PORTS[@]}"; do
  info ":$p $(curl -s "http://127.0.0.1:$p/admin/v1/stats")"
done

step "a denied response, for inspection"
curl -s -D - -o /dev/null -H "X-Client-ID: $CLIENT" "http://127.0.0.1:${PORTS[0]}/order" | head -12

echo
echo "DEMO PASSED"
