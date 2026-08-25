#!/usr/bin/env bash
set -euo pipefail

SAVEPOINT_DIR="${SAVEPOINT_DIR:-file:///opt/flink/state/savepoints}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
FLINK_JOB_NAME="${FLINK_JOB_NAME:-flink-kraft-realtime-lab}"
FLINK_JOB_ID="${FLINK_JOB_ID:-}"

JOB_ID="$(
  curl -fsS "${FLINK_URL}/jobs/overview" \
    | REQUESTED_JOB_ID="${FLINK_JOB_ID}" REQUESTED_JOB_NAME="${FLINK_JOB_NAME}" python3 -c '
import json
import os
import sys

jobs = [job for job in json.load(sys.stdin).get("jobs", []) if job.get("state") == "RUNNING"]
requested_id = os.environ["REQUESTED_JOB_ID"]
requested_name = os.environ["REQUESTED_JOB_NAME"]

if requested_id:
    matches = [job for job in jobs if job.get("jid") == requested_id]
    selector = f"id={requested_id}"
else:
    matches = [job for job in jobs if job.get("name") == requested_name]
    selector = f"name={requested_name}"

if len(matches) != 1:
    available = ", ".join(
        "{}:{}".format(job.get("jid"), job.get("name")) for job in jobs
    ) or "none"
    raise SystemExit(
        f"expected exactly one RUNNING Flink job for {selector}; "
        f"matches={len(matches)}, running={available}"
    )

print(matches[0]["jid"])
'
)"

echo "triggering savepoint: jobId=${JOB_ID}, target=${SAVEPOINT_DIR}"
docker compose exec -T flink-jobmanager flink savepoint "${JOB_ID}" "${SAVEPOINT_DIR}"
