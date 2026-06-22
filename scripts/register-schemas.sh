#!/usr/bin/env sh
set -eu

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8085}"

wait_for_schema_registry() {
  retries=60
  while [ "$retries" -gt 0 ]; do
    if curl -fsS "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
      return 0
    fi
    retries=$((retries - 1))
    sleep 2
  done
  echo "schema registry is not ready: ${SCHEMA_REGISTRY_URL}" >&2
  return 1
}

register_schema() {
  subject="$1"
  schema_file="$2"
  payload="$(awk 'BEGIN { printf "{\"schema\":\"" } { gsub(/\\/,"\\\\"); gsub(/"/,"\\\""); printf "%s\\n", $0 } END { printf "\"}" }' "$schema_file")"
  curl -fsS \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "$payload" \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions"
  echo
}

wait_for_schema_registry
register_schema "transactions.raw-value" "/schemas/transactions-raw-value.avsc"
register_schema "transactions.replay-value" "/schemas/transactions-raw-value.avsc"
register_schema "alerts.fraud-value" "/schemas/alerts-fraud-value.avsc"
register_schema "transactions.aggregates-value" "/schemas/transactions-aggregates-value.avsc"
register_schema "transactions.dlq-value" "/schemas/transactions-dlq-value.avsc"

curl -fsS "${SCHEMA_REGISTRY_URL}/subjects"
echo
