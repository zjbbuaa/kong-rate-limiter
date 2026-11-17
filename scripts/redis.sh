#!/usr/bin/env bash
# Start/stop a project-local Redis instance on 127.0.0.1:6379.
# Data lives in .redis/ (ephemeral; persistence disabled on purpose).
set -euo pipefail
cd "$(dirname "$0")/.."

REDIS_DIR="$PWD/.redis"
REDIS_BIN="$(brew --prefix redis 2>/dev/null || echo /opt/homebrew/opt/redis)/bin"
mkdir -p "$REDIS_DIR"

cmd="${1:-start}"
case "$cmd" in
  start)
    if "$REDIS_BIN/redis-cli" -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
      echo "redis already running on 127.0.0.1:6379"
      exit 0
    fi
    "$REDIS_BIN/redis-server" \
      --bind 127.0.0.1 --port 6379 \
      --dir "$REDIS_DIR" --dbfilename dump.rdb \
      --save '' --appendonly no \
      --daemonize yes --pidfile "$REDIS_DIR/redis.pid" \
      --logfile "$REDIS_DIR/redis.log"
    for _ in $(seq 1 50); do
      "$REDIS_BIN/redis-cli" -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1 && break
      sleep 0.1
    done
    "$REDIS_BIN/redis-cli" -h 127.0.0.1 -p 6379 ping
    ;;
  stop)
    "$REDIS_BIN/redis-cli" -h 127.0.0.1 -p 6379 shutdown nosave 2>/dev/null || true
    rm -f "$REDIS_DIR/redis.pid"
    echo "redis stopped"
    ;;
  cli)
    exec "$REDIS_BIN/redis-cli" -h 127.0.0.1 -p 6379 "${@:2}"
    ;;
  *)
    echo "usage: $0 [start|stop|cli ...]" >&2
    exit 2
    ;;
esac
