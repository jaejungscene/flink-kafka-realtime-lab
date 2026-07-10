#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
KAFKA_GROUP="${KAFKA_GROUP:-flink-realtime-lab}"

echo "== realtime lab load snapshot =="
date -u +"timestamp_utc=%Y-%m-%dT%H:%M:%SZ"

echo
echo "== api health =="
curl -fsS "${API_URL}/health"
echo

echo
echo "== flink jobs =="
jobs_json="$(curl -fsS "${FLINK_URL}/jobs" 2>/dev/null || true)"
if [ -z "${jobs_json}" ]; then
  echo "Flink REST API is not reachable at ${FLINK_URL}"
else
  echo "${jobs_json}" | python3 -m json.tool
fi

running_job_id=""
if [ -n "${jobs_json}" ]; then
  running_job_id="$(
    printf '%s' "${jobs_json}" | python3 -c '
import json
import sys

jobs = json.load(sys.stdin).get("jobs", [])
for job in jobs:
    if job.get("status") == "RUNNING":
        print(job.get("id"))
        break
' 2>/dev/null || true
  )"
fi

if [ -n "${running_job_id}" ]; then
  echo
  echo "== flink vertices =="
  curl -fsS "${FLINK_URL}/jobs/${running_job_id}" \
    | python3 -c '
import json
import sys

data = json.load(sys.stdin)
for vertex in data.get("vertices", []):
    print(
        "{} | status={} | parallelism={} | durationMs={}".format(
            vertex.get("name"),
            vertex.get("status"),
            vertex.get("parallelism"),
            vertex.get("duration", 0),
        )
    )
'

  echo
  echo "== flink backpressure probes =="
  curl -fsS "${FLINK_URL}/jobs/${running_job_id}" \
    | python3 -c '
import json
import sys

data = json.load(sys.stdin)
for vertex in data.get("vertices", []):
    print(vertex.get("id"), vertex.get("name"), sep="\t")
' \
    | while IFS=$'\t' read -r vertex_id vertex_name; do
        [ -n "${vertex_id}" ] || continue
        response="$(curl -fsS "${FLINK_URL}/jobs/${running_job_id}/vertices/${vertex_id}/backpressure" 2>/dev/null || true)"
        if [ -z "${response}" ]; then
          echo "${vertex_name} | backpressure endpoint unavailable"
        else
          echo "${response}" | VERTEX_NAME="${vertex_name}" python3 -c '
import json
import os
import sys

data = json.load(sys.stdin)
level = data.get("backpressure-level") or data.get("backpressureLevel") or "UNKNOWN"
ratio = data.get("backpressured-ratio") or data.get("backpressuredRatio") or "n/a"
print(os.environ["VERTEX_NAME"] + " | level=" + str(level) + " | ratio=" + str(ratio))
'
        fi
      done
else
  echo
  echo "== flink vertices =="
  echo "no RUNNING Flink job found yet"
fi

echo
echo "== kafka consumer lag =="
lag_output="$(
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 \
    --describe \
    --group "${KAFKA_GROUP}" 2>&1
)" || true
if echo "${lag_output}" | grep -q "GroupIdNotFoundException"; then
  echo "consumer group ${KAFKA_GROUP} not found yet"
elif [ -n "${lag_output}" ]; then
  echo "${lag_output}"
fi

echo
echo "== api metrics summary =="
curl -fsS "${API_URL}/metrics" \
  | python3 -c '
import re
import sys

topic_total = re.compile(r"realtime_lab_kafka_topic_messages_total\{topic=\"([^\"]+)\"\} ([0-9.]+)")
lag = re.compile(r"realtime_lab_kafka_consumer_lag\{group=\"([^\"]+)\",topic=\"([^\"]+)\",partition=\"([^\"]+)\"\} ([0-9.]+)")

topic_rows = []
lag_rows = []
for line in sys.stdin:
    topic_match = topic_total.match(line.strip())
    if topic_match:
        topic_rows.append((topic_match.group(1), float(topic_match.group(2))))
        continue
    lag_match = lag.match(line.strip())
    if lag_match:
        lag_rows.append((lag_match.group(2), lag_match.group(3), float(lag_match.group(4))))

for topic, count in sorted(topic_rows):
    print(f"topic_total {topic} {count:.0f}")

if lag_rows:
    for topic, partition, value in sorted(lag_rows):
        print(f"consumer_lag {topic} partition={partition} lag={value:.0f}")
else:
    print("consumer_lag no committed offsets yet")
'
