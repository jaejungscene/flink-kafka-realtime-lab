#!/usr/bin/env sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

retries=60
while [ "$retries" -gt 0 ]; do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  retries=$((retries - 1))
  sleep 2
done

if [ "$retries" -eq 0 ]; then
  echo "kafka connect is not ready: ${CONNECT_URL}" >&2
  exit 1
fi

curl -fsS \
  -X PUT \
  -H "Content-Type: application/json" \
  --data @/cdc/postgres-source-connector.json \
  "${CONNECT_URL}/connectors/merchant-risk-profiles-source/config"
echo
curl -fsS "${CONNECT_URL}/connectors/merchant-risk-profiles-source/status"
echo
