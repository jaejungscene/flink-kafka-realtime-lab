#!/usr/bin/env bash
set -euo pipefail

jobmanager_address="${FLINK_JOBMANAGER_ADDRESS:-flink-jobmanager:8081}"
job_jar="${FLINK_JOB_JAR:-/opt/flink/usrlib/realtime-lab-flink-job.jar}"
max_attempts="${FLINK_SUBMIT_MAX_ATTEMPTS:-30}"

for ((attempt = 1; attempt <= max_attempts; attempt += 1)); do
  if flink list -m "${jobmanager_address}" >/dev/null 2>&1; then
    exec flink run -d -m "${jobmanager_address}" "${job_jar}" "$@"
  fi

  echo "waiting for Flink JobManager (${attempt}/${max_attempts})"
  sleep 2
done

echo "Flink JobManager did not become ready: ${jobmanager_address}" >&2
exit 1
