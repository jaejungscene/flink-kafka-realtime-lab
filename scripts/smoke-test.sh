#!/usr/bin/env bash
set -euo pipefail

echo "checking Kafka topics"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

echo "checking Flink jobs"
curl -fsS http://localhost:8081/jobs | sed 's/^/flink: /'

echo "checking API health"
curl -fsS http://localhost:8000/health | sed 's/^/api: /'

echo "checking alert topic through API"
curl -fsS "http://localhost:8000/topics/alerts.fraud/messages?limit=5&timeout_seconds=2" | sed 's/^/alerts: /'

echo "checking aggregate topic through API"
curl -fsS "http://localhost:8000/topics/transactions.aggregates/messages?limit=5&timeout_seconds=2" | sed 's/^/aggregates: /'

echo "checking DLQ topic through API"
curl -fsS "http://localhost:8000/topics/transactions.dlq/messages?limit=5&timeout_seconds=2" | sed 's/^/dlq: /'
