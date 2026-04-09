#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$ROOT_DIR/server.pid"
LOG_FILE="$ROOT_DIR/server.log"
BASE_URL="${AECO_MONITOR_BASE_URL:-http://127.0.0.1:8000}"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" >/dev/null 2>&1; then
    echo "AECO test service is already running on PID $PID"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

cd "$ROOT_DIR"
nohup python3 server.py >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
PID="$(cat "$PID_FILE")"

for _ in {1..60}; do
  if curl -fsS "$BASE_URL/api/settings" >/dev/null 2>&1; then
    echo "AECO test service started"
    echo "PID: $PID"
    echo "URL: $BASE_URL"
    echo "Log: $LOG_FILE"
    exit 0
  fi

  if ! kill -0 "$PID" >/dev/null 2>&1; then
    echo "AECO test service exited before becoming ready"
    [[ -f "$LOG_FILE" ]] && tail -n 20 "$LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
  fi

  sleep 1
done

echo "AECO test service did not become ready within 60 seconds"
tail -n 20 "$LOG_FILE" || true
exit 1
