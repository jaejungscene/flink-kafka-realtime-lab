#!/usr/bin/env bash
set -euo pipefail

curl_api() {
  if [ -n "${API_TOKEN:-}" ]; then
    curl -fsS -H "X-API-Token: ${API_TOKEN}" "$@"
  else
    curl -fsS "$@"
  fi
}

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
    response="$(curl_api "http://localhost:8000/topics/${topic}/messages?limit=20&timeout_seconds=${api_timeout_seconds}&from_beginning=true")"
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

echo "checking DLQ summary API"
curl_api "http://localhost:8000/dlq/summary?limit=100&timeout_seconds=3&from_beginning=true" \
  | tee /tmp/realtime-lab-dlq-summary.json \
  | sed 's/^/dlq-summary: /'
python3 - <<'PY'
import json

with open("/tmp/realtime-lab-dlq-summary.json", encoding="utf-8") as fp:
    data = json.load(fp)

if data.get("scanned", 0) < 1:
    raise SystemExit("DLQ summary did not scan any records")
if not data.get("byErrorType"):
    raise SystemExit("DLQ summary did not include error type breakdown")
PY

echo "checking DLQ replay preview API"
curl_api -X POST "http://localhost:8000/dlq/replay" \
  -H "content-type: application/json" \
  -d '{"max_messages":1,"scan_limit":200,"timeout_seconds":5,"dry_run":true}' \
  | tee /tmp/realtime-lab-dlq-replay-preview.json \
  | sed 's/^/dlq-replay-preview: /'
python3 - <<'PY'
import json

with open("/tmp/realtime-lab-dlq-replay-preview.json", encoding="utf-8") as fp:
    data = json.load(fp)

if data.get("dryRun") is not True:
    raise SystemExit("DLQ replay preview must use dryRun=true")
if data.get("scanned", 0) < 1:
    raise SystemExit("DLQ replay preview did not scan any records")
PY

echo "checking DLQ replay API"
API_REPLAY_MAX_MESSAGES=1 \
API_REPLAY_SCAN_LIMIT=200 \
API_REPLAY_TIMEOUT_SECONDS=5 \
python3 scripts/replay-dlq-api.py \
  | tee /tmp/realtime-lab-dlq-replay.json \
  | sed 's/^/dlq-replay: /'
python3 - <<'PY'
import json

with open("/tmp/realtime-lab-dlq-replay.json", encoding="utf-8") as fp:
    data = json.load(fp)

if data.get("dryRun") is not False:
    raise SystemExit("DLQ replay execution must use dryRun=false")
if data.get("replayed", 0) < 1:
    raise SystemExit("DLQ replay API did not publish any replayable records")
PY

echo "checking replay topic through API"
wait_for_topic_count "transactions.replay" 1

echo "smoke test passed"
