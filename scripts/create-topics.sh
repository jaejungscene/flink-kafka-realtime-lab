#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local retention_ms="$3"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}"
}

create_compacted_topic() {
  local topic="$1"
  local partitions="$2"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "cleanup.policy=compact" \
    --config "retention.ms=604800000"
}

create_topic "transactions.raw" 3 86400000
create_topic "transactions.replay" 3 86400000
create_topic "transactions.aggregates" 3 86400000
create_topic "transactions.aggregates.sql" 3 86400000
create_topic "alerts.fraud" 3 604800000
create_topic "transactions.dlq" 1 604800000
create_compacted_topic "merchant_risk_profiles" 3
create_compacted_topic "connect-configs" 1
create_compacted_topic "connect-offsets" 3
create_compacted_topic "connect-status" 3

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVER}" --list
