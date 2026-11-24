#!/usr/bin/env bash
# Run a local ObjectVault cluster: PostgreSQL, Redis, N storage nodes, gateway.
#   scripts/cluster.sh start [N]     (default N=3)
#   scripts/cluster.sh stop
#   scripts/cluster.sh kill-node s2  (SIGKILL one storage node)
#   scripts/cluster.sh start-node s2
# Extra flags: GATEWAY_ARGS="--no-cache ...", STORAGE_ARGS="--scrub-interval 0 ...".
# PostgreSQL and Redis are both reached over Unix sockets (same host).
set -euo pipefail
export LC_ALL=${LC_ALL:-en_US.UTF-8}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${OV_DATA:-$ROOT/data}"
BIN="$ROOT/build"
PG_BIN="$(brew --prefix postgresql@16)/bin"
PG_PORT=5433
REDIS_PORT=6380
mkdir -p "$DATA/logs"

node_addr() { echo "127.0.0.1:$((9000 + ${1#s}))"; }

start_node() {
  local id=$1
  "$BIN/ov-storage" --id "$id" --listen "$(node_addr "$id")" --dir "$DATA/$id" \
    --metrics "127.0.0.1:$((9100 + ${id#s}))" ${STORAGE_ARGS:-} >>"$DATA/logs/$id.log" 2>&1 &
  echo $! >"$DATA/$id.pid"
}

case "${1:-}" in
start)
  N=${2:-3}
  if [ ! -d "$DATA/pg" ]; then
    "$PG_BIN/initdb" -D "$DATA/pg" -U postgres >/dev/null
    printf "max_connections = 400\nshared_buffers = 512MB\nsynchronous_commit = on\n" >>"$DATA/pg/postgresql.conf"
  fi
  "$PG_BIN/pg_ctl" -D "$DATA/pg" -o "-p $PG_PORT -k /tmp" -l "$DATA/logs/pg.log" -w start >/dev/null
  "$PG_BIN/createdb" -h /tmp -p $PG_PORT -U postgres objectvault 2>/dev/null || true
  redis-server --port $REDIS_PORT --unixsocket /tmp/ov-redis.sock --unixsocketperm 700 \
    --save "" --appendonly no --daemonize yes --pidfile "$DATA/redis.pid" \
    --logfile "$DATA/logs/redis.log"
  NODES=""
  for i in $(seq 1 "$N"); do
    start_node "s$i"
    NODES="${NODES:+$NODES,}s$i=$(node_addr "s$i")"
  done
  echo "$N" >"$DATA/nodes"
  sleep 0.5
  # shellcheck disable=SC2086
  "$BIN/ov-gateway" --listen 127.0.0.1:8080 --metrics 127.0.0.1:9200 --nodes "$NODES" \
    --pg "host=/tmp port=$PG_PORT user=postgres dbname=objectvault" --redis unix:/tmp/ov-redis.sock \
    ${GATEWAY_ARGS:-} >>"$DATA/logs/gateway.log" 2>&1 &
  echo $! >"$DATA/gateway.pid"
  for _ in $(seq 1 50); do curl -sf localhost:8080/healthz >/dev/null && break; sleep 0.1; done
  echo "ObjectVault up: gateway http://127.0.0.1:8080 ($N storage nodes), metrics :9200 and :9101-:910$N"
  ;;
stop)
  for f in "$DATA"/*.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true; rm -f "$f"; done
  "$PG_BIN/pg_ctl" -D "$DATA/pg" -m fast stop >/dev/null 2>&1 || true
  echo "stopped"
  ;;
kill-node)
  kill -9 "$(cat "$DATA/$2.pid")" && rm -f "$DATA/$2.pid" && echo "SIGKILL $2"
  ;;
start-node)
  start_node "$2" && echo "started $2"
  ;;
wipe)
  "$0" stop >/dev/null 2>&1 || true
  rm -rf "$DATA"
  ;;
*)
  echo "usage: $0 {start [N]|stop|kill-node ID|start-node ID|wipe}" >&2
  exit 2
  ;;
esac
