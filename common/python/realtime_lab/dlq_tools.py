from __future__ import annotations

import json
import math
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any


def now_millis() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        return int(value) if math.isfinite(value) and value.is_integer() else None
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
    event, _ = _replay_candidate(dlq_value)
    if event is None:
        return None

    amount = _float_or_none(event.get("amount"))
    event_time = _int_or_none(event.get("eventTime"))
    ml_fraud_score = _float_or_none(event.get("mlFraudScore", 0.0))
    ip_risk = _int_or_none(event.get("ipRisk", 0))
    if amount is None or event_time is None or ml_fraud_score is None or ip_risk is None:
        return None

    deterministic_event_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"{source_topic}:{source_partition}:{source_offset}",
    )
    event["eventId"] = event.get("eventId") or f"replay-{deterministic_event_id}"
    event["userId"] = str(event["userId"]).strip()
    event["eventTime"] = event_time
    event["amount"] = amount
    event["mlFraudScore"] = ml_fraud_score
    event["ipRisk"] = ip_risk
    event["replayId"] = f"{replay_run_id}-{source_partition}-{source_offset}"
    event["replayRunId"] = replay_run_id
    event["replaySourceTopic"] = source_topic
    event["replaySourcePartition"] = source_partition
    event["replaySourceOffset"] = source_offset
    event["replayedFromDlqAt"] = now_millis()
    return event


def replay_block_reason(dlq_value: dict[str, Any]) -> str | None:
    _, reason = _replay_candidate(dlq_value)
    return reason


def _replay_candidate(dlq_value: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    error_type = str(dlq_value.get("errorType") or "")
    if error_type != "PARSE_OR_VALIDATION_ERROR":
        return None, f"errorType {error_type or 'UNKNOWN'} requires a separate remediation path"

    raw_value = dlq_value.get("rawValue")
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None, "rawValue is missing"

    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        return None, "rawValue is not valid JSON"
    if not isinstance(event, dict):
        return None, "rawValue must contain a JSON object"

    user_id = event.get("userId")
    if not isinstance(user_id, str) or not user_id.strip():
        return None, "userId cannot be inferred safely"

    amount = _float_or_none(event.get("amount"))
    if amount is None or amount < 0:
        return None, "amount must be a finite non-negative number"
    event_time = _int_or_none(event.get("eventTime"))
    if event_time is None or event_time <= 0:
        return None, "eventTime must be preserved as positive epoch millis"
    score = _float_or_none(event.get("mlFraudScore", 0.0))
    if score is None or not 0 <= score <= 1:
        return None, "mlFraudScore must be between 0 and 1"
    ip_risk = _int_or_none(event.get("ipRisk", 0))
    if ip_risk is None or not 0 <= ip_risk <= 100:
        return None, "ipRisk must be between 0 and 100"

    return event, None


def to_dlq_sample(record: dict[str, Any], replay_run_id: str) -> dict[str, Any]:
    value = record.get("value")
    if not isinstance(value, dict):
        value = {}

    raw_value = str(value.get("rawValue") or "")
    block_reason = replay_block_reason(value)
    replayable = block_reason is None

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
        "replayBlockReason": block_reason,
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
