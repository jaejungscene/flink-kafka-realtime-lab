#!/usr/bin/env bash
set -euo pipefail

docker compose kill flink-taskmanager
echo "flink-taskmanager killed. waiting before restart..."
sleep 5
docker compose up -d flink-taskmanager
echo "flink-taskmanager restarted. check recovery with: curl http://localhost:8081/jobs"
