from __future__ import annotations

import json
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any


def now_millis() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_for_replay(
    dlq_value: dict[str, Any],
    source_topic: str,
    source_partition: int,
    source_offset: int,
    replay_run_id: str,
) -> dict[str, Any] | None:
    raw_value = dlq_value.get("rawValue")
    if not raw_value:
        return None

    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        return None

    amount = _float_or_none(event.get("amount", 0.0))
    ml_fraud_score = _float_or_none(event.get("mlFraudScore", 0.0))
    ip_risk = _int_or_none(event.get("ipRisk", 0))
    if amount is None or ml_fraud_score is None or ip_risk is None:
        return None

    event["eventId"] = event.get("eventId") or f"replay-{uuid.uuid4()}"
    event["userId"] = event.get("userId") or "user-replayed"
    event["merchantId"] = event.get("merchantId") or "merchant-replayed"
    event["category"] = event.get("category") or "replay"
    event["eventTime"] = now_millis()
    event["amount"] = max(amount, 0.0)
    event["currency"] = event.get("currency") or "USD"
    event["country"] = event.get("country") or "UNKNOWN"
    event["channel"] = event.get("channel") or "replay"
    event["deviceId"] = event.get("deviceId") or "device-replayed"
    event["mlFraudScore"] = ml_fraud_score
    event["paymentStatus"] = event.get("paymentStatus") or "REPLAYED"
    event["ipRisk"] = ip_risk
    event["replayId"] = f"{replay_run_id}-{source_partition}-{source_offset}"
    event["replayRunId"] = replay_run_id
    event["replaySourceTopic"] = source_topic
    event["replaySourcePartition"] = source_partition
    event["replaySourceOffset"] = source_offset
    event["replayedFromDlqAt"] = now_millis()
    return event


def to_dlq_sample(record: dict[str, Any], replay_run_id: str) -> dict[str, Any]:
    value = record.get("value")
    if not isinstance(value, dict):
        value = {}

    raw_value = str(value.get("rawValue") or "")
    replayable = normalize_for_replay(
        value,
        str(record.get("topic") or ""),
        int(record.get("partition") or 0),
        int(record.get("offset") or 0),
        replay_run_id,
    ) is not None

    return {
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "key": record.get("key"),
        "errorType": value.get("errorType", "UNKNOWN"),
        "reason": value.get("reason", "UNKNOWN"),
        "sourceTopic": value.get("sourceTopic"),
        "replayTopic": value.get("replayTopic"),
        "observedAt": value.get("observedAt"),
        "replayable": replayable,
        "rawValuePreview": raw_value[:240],
    }


def summarize_dlq_records(records: list[dict[str, Any]], sample_limit: int = 10) -> dict[str, Any]:
    replay_run_id = f"summary-{uuid.uuid4()}"
    by_error_type: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    replayable_by_error_type: defaultdict[str, int] = defaultdict(int)
    samples: list[dict[str, Any]] = []
    replayable_count = 0

    for record in records:
        sample = to_dlq_sample(record, replay_run_id)
        error_type = str(sample["errorType"])
        reason = str(sample["reason"])
        by_error_type[error_type] += 1
        by_reason[reason] += 1

        if sample["replayable"]:
            replayable_count += 1
            replayable_by_error_type[error_type] += 1

        if len(samples) < sample_limit:
            samples.append(sample)

    return {
        "scanned": len(records),
        "replayable": replayable_count,
        "notReplayable": len(records) - replayable_count,
        "byErrorType": [
            {
                "errorType": error_type,
                "count": count,
                "replayable": replayable_by_error_type.get(error_type, 0),
            }
            for error_type, count in by_error_type.most_common()
        ],
        "byReason": [
            {"reason": reason, "count": count}
            for reason, count in by_reason.most_common(10)
        ],
        "samples": samples,
    }
