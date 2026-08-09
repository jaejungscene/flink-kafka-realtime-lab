#!/usr/bin/env bash
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose}
KAFKA_BIN=/opt/kafka/bin/kafka-console-consumer.sh
TOPIC=${CDC_TOPIC:-merchant_risk_profiles}
MERCHANT_ID="cdc-smoke-$(date +%s)"
ATTEMPTS=${CDC_SMOKE_ATTEMPTS:-20}

read_topic() {
  $COMPOSE exec -T kafka "$KAFKA_BIN" \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" \
    --from-beginning \
    --timeout-ms 2000 \
    2>/dev/null || true
}

wait_for_record() {
  local expected_deleted=$1
  local output

  for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
    output=$(read_topic)
    if printf '%s\n' "$output" | \
      MERCHANT_ID="$MERCHANT_ID" EXPECT_DELETED="$expected_deleted" python3 -c '
import json
import os
import sys

merchant_id = os.environ["MERCHANT_ID"]
expected_deleted = os.environ["EXPECT_DELETED"] == "true"
for line in sys.stdin:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        continue
    if not isinstance(value, dict) or value.get("merchant_id") != merchant_id:
        continue
    deleted = value.get("__deleted") in (True, "true")
    if deleted == expected_deleted:
        raise SystemExit(0)
raise SystemExit(1)
'; then
      return 0
    fi
    sleep 2
  done

  echo "CDC record not observed: merchant=$MERCHANT_ID deleted=$expected_deleted" >&2
  return 1
}

$COMPOSE exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d realtime_lab -c "
  insert into merchant_risk_profiles
    (merchant_id, risk_tier, risk_multiplier, manual_review_required)
  values ('$MERCHANT_ID', 'HIGH', 1.900, true);
"
wait_for_record false

$COMPOSE exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d realtime_lab -c "
  delete from merchant_risk_profiles where merchant_id = '$MERCHANT_ID';
"
wait_for_record true

echo "CDC insert/delete smoke passed: merchant=$MERCHANT_ID topic=$TOPIC"
