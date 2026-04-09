#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$ROOT_DIR/server.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No pid file found"
  exit 0
fi

PID="$(cat "$PID_FILE")"
if kill -0 "$PID" >/dev/null 2>&1; then
  kill "$PID"
  echo "Stopped AECO test service PID $PID"
else
  echo "Process $PID is not running"
fi

rm -f "$PID_FILE"
