#!/usr/bin/env bash
set -euo pipefail

RUN_SECONDS="${LOAD_RUN_SECONDS:-180}"
EVENTS_PER_SECOND="${LOAD_EVENTS_PER_SECOND:-150}"
SNAPSHOT_INTERVAL_SECONDS="${LOAD_SNAPSHOT_INTERVAL_SECONDS:-30}"
producer_pid=""

cleanup() {
  if [ -n "${producer_pid}" ] && kill -0 "${producer_pid}" 2>/dev/null; then
    kill "${producer_pid}" 2>/dev/null || true
  fi
}

trap cleanup INT TERM EXIT

echo "starting load experiment: runSeconds=${RUN_SECONDS}, eventsPerSecond=${EVENTS_PER_SECOND}"
./scripts/load-snapshot.sh

docker compose run --rm \
  -e RUN_SECONDS="${RUN_SECONDS}" \
  -e EVENTS_PER_SECOND="${EVENTS_PER_SECOND}" \
  generator &
producer_pid=$!

while kill -0 "${producer_pid}" 2>/dev/null; do
  sleep "${SNAPSHOT_INTERVAL_SECONDS}"
  ./scripts/load-snapshot.sh || true
done

wait "${producer_pid}"
producer_pid=""

echo "load generator finished"
./scripts/load-snapshot.sh
