#!/usr/bin/env bash
set -Eeuo pipefail

GENERATOR_RUN_SECONDS="${CI_GENERATOR_RUN_SECONDS:-80}"
GENERATOR_EVENTS_PER_SECOND="${CI_GENERATOR_EVENTS_PER_SECOND:-40}"
FLINK_WAIT_ATTEMPTS="${CI_FLINK_WAIT_ATTEMPTS:-60}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-realtime-lab-ci}"

compose() {
  docker compose "$@"
}

print_failure_context() {
  echo "::group::docker compose ps"
  compose ps || true
  echo "::endgroup::"

  echo "::group::docker compose logs"
  compose logs --no-color --tail=300 \
    kafka \
    topic-init \
    flink-jobmanager \
    flink-taskmanager \
    flink-submit \
    api \
    generator || true
  echo "::endgroup::"
}

cleanup() {
  local status=$?
  if [ "${status}" -ne 0 ]; then
    print_failure_context
  fi
  compose down -v --remove-orphans
  exit "${status}"
}

wait_for_flink_job() {
  local response

  for _ in $(seq 1 "${FLINK_WAIT_ATTEMPTS}"); do
    response="$(curl -fsS http://localhost:8081/jobs 2>/dev/null || true)"
    if [ -n "${response}" ] && echo "${response}" | python3 -c '
import json
import sys

jobs = json.load(sys.stdin).get("jobs", [])
raise SystemExit(0 if any(job.get("status") == "RUNNING" for job in jobs) else 1)
' 2>/dev/null; then
      return 0
    fi
    sleep 3
  done

  echo "no RUNNING Flink job found within wait window" >&2
  return 1
}

trap cleanup EXIT

echo "resetting compose environment"
compose down -v --remove-orphans

echo "building runtime images"
compose build flink-jobmanager flink-taskmanager flink-submit api generator

echo "starting core services"
compose up -d kafka topic-init flink-jobmanager flink-taskmanager flink-submit api

echo "waiting for Flink job to become RUNNING"
wait_for_flink_job

echo "producing deterministic CI workload"
compose run --rm \
  -e RUN_SECONDS="${GENERATOR_RUN_SECONDS}" \
  -e EVENTS_PER_SECOND="${GENERATOR_EVENTS_PER_SECOND}" \
  -e INCLUDE_BAD_EVENTS=true \
  generator

echo "running smoke assertions"
SMOKE_TOPIC_ATTEMPTS="${SMOKE_TOPIC_ATTEMPTS:-35}" \
SMOKE_TOPIC_SLEEP_SECONDS="${SMOKE_TOPIC_SLEEP_SECONDS:-3}" \
  ./scripts/smoke-test.sh

echo "CI E2E smoke test passed"
