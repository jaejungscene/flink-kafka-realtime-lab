#!/usr/bin/env bash
set -euo pipefail

echo "checking Kafka topics"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo "checking Flink jobs"
curl -fsS http://localhost:8081/jobs | sed 's/^/flink: /'
curl -fsS http://localhost:8081/jobs | python3 -c '
import json
import sys

jobs = json.load(sys.stdin).get("jobs", [])
if not any(job.get("status") == "RUNNING" for job in jobs):
    raise SystemExit("no RUNNING Flink job found")
'

echo "checking API health"
curl -fsS http://localhost:8000/health | sed 's/^/api: /'

wait_for_topic_count() {
  local topic="$1"
  local min_count="$2"
  local attempts="${3:-${SMOKE_TOPIC_ATTEMPTS:-20}}"
  local sleep_seconds="${SMOKE_TOPIC_SLEEP_SECONDS:-3}"
  local api_timeout_seconds="${SMOKE_API_TIMEOUT_SECONDS:-2}"
  local response

  for _ in $(seq 1 "${attempts}"); do
    response="$(curl -fsS "http://localhost:8000/topics/${topic}/messages?limit=20&timeout_seconds=${api_timeout_seconds}&from_beginning=true")"
    echo "${response}" | sed "s/^/${topic}: /"
    if echo "${response}" | python3 -c "import json,sys; data=json.load(sys.stdin); raise SystemExit(0 if data.get('count', 0) >= ${min_count} else 1)"; then
      return 0
    fi
    sleep "${sleep_seconds}"
  done

  echo "topic ${topic} did not reach count >= ${min_count}" >&2
  return 1
}

echo "checking alert topic through API"
wait_for_topic_count "alerts.fraud" 1

echo "checking aggregate topic through API"
wait_for_topic_count "transactions.aggregates" 1

echo "checking DLQ topic through API"
wait_for_topic_count "transactions.dlq" 1

echo "smoke test passed"
