#!/usr/bin/env bash
set -euo pipefail

docker compose kill flink-taskmanager
echo "flink-taskmanager killed; restarting it now"
docker compose up -d flink-taskmanager

deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  if curl -fsS http://localhost:8081/jobs/overview 2>/dev/null \
    | python3 -c '
import json
import sys

jobs = json.load(sys.stdin).get("jobs", [])
raise SystemExit(0 if any(job.get("state") == "RUNNING" for job in jobs) else 1)
'; then
    echo "Flink job returned to RUNNING state"
    exit 0
  fi
  sleep 3
done

echo "Flink job did not return to RUNNING state within 90 seconds" >&2
exit 1
