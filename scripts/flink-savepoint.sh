#!/usr/bin/env bash
set -euo pipefail

JOB_ID="$(curl -fsS http://localhost:8081/jobs | python3 -c 'import json,sys; jobs=json.load(sys.stdin).get("jobs", []); running=[j for j in jobs if j.get("status") == "RUNNING"]; print(running[0]["id"] if running else "")')"

if [ -z "${JOB_ID}" ]; then
  echo "no running Flink job found" >&2
  exit 1
fi

docker compose exec flink-jobmanager flink savepoint "${JOB_ID}" /tmp/flink-savepoints
