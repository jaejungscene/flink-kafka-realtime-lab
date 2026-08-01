from __future__ import annotations

import json
import math
import re
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any

REPLAY_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{2,79}$")
DEFAULT_MAX_FUTURE_SKEW_MILLIS = 300_000


def now_millis() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


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
    *,
    current_time_millis: int | None = None,
    max_future_skew_millis: int = DEFAULT_MAX_FUTURE_SKEW_MILLIS,
) -> dict[str, Any] | None:
    validate_replay_run_id(replay_run_id)
    replay_time = now_millis() if current_time_millis is None else current_time_millis
    event, _ = _replay_candidate(
        dlq_value,
        current_time_millis=replay_time,
        max_future_skew_millis=max_future_skew_millis,
    )
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
    event_id = event.get("eventId")
    event["eventId"] = (
        f"replay-{deterministic_event_id}"
        if event_id is None or not event_id.strip()
        else event_id.strip()
    )
    event["userId"] = str(event["userId"]).strip()
    event["schemaVersion"] = _int_or_none(event.get("schemaVersion", 1))
    event["eventTime"] = event_time
    event["amount"] = amount
    event["mlFraudScore"] = ml_fraud_score
    event["ipRisk"] = ip_risk
    event["replayId"] = f"{replay_run_id}-{source_partition}-{source_offset}"
    event["replayRunId"] = replay_run_id
    event["replaySourceTopic"] = source_topic
    event["replaySourcePartition"] = source_partition
    event["replaySourceOffset"] = source_offset
    event["replayedFromDlqAt"] = replay_time
    return event


def replay_block_reason(
    dlq_value: dict[str, Any],
    *,
    current_time_millis: int | None = None,
    max_future_skew_millis: int = DEFAULT_MAX_FUTURE_SKEW_MILLIS,
) -> str | None:
    _, reason = _replay_candidate(
        dlq_value,
        current_time_millis=(
            now_millis() if current_time_millis is None else current_time_millis
        ),
        max_future_skew_millis=max_future_skew_millis,
    )
    return reason


def validate_replay_run_id(replay_run_id: str) -> None:
    if not REPLAY_RUN_ID_PATTERN.fullmatch(replay_run_id):
        raise ValueError(
            "replay run ID must be 3-80 characters using letters, numbers, "
            "dot, underscore, or hyphen"
        )


def _replay_candidate(
    dlq_value: dict[str, Any],
    *,
    current_time_millis: int,
    max_future_skew_millis: int,
) -> tuple[dict[str, Any] | None, str | None]:
    if max_future_skew_millis < 0:
        raise ValueError("max future skew must not be negative")
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

    event_id = event.get("eventId")
    if event_id is not None and not isinstance(event_id, str):
        return None, "eventId must be a string when present"
    if isinstance(event_id, str) and len(event_id.strip()) > 256:
        return None, "eventId must not exceed 256 characters"

    user_id = event.get("userId")
    if not isinstance(user_id, str) or not user_id.strip():
        return None, "userId cannot be inferred safely"
    if len(user_id.strip()) > 256:
        return None, "userId must not exceed 256 characters"

    amount = _float_or_none(event.get("amount"))
    if amount is None or amount < 0:
        return None, "amount must be a finite non-negative number"
    event_time = _int_or_none(event.get("eventTime"))
    if event_time is None or event_time <= 0:
        return None, "eventTime must be preserved as positive epoch millis"
    if event_time > current_time_millis + max_future_skew_millis:
        return None, "eventTime exceeds the configured future-skew limit"
    score = _float_or_none(event.get("mlFraudScore", 0.0))
    if score is None or not 0 <= score <= 1:
        return None, "mlFraudScore must be between 0 and 1"
    ip_risk = _int_or_none(event.get("ipRisk", 0))
    if ip_risk is None or not 0 <= ip_risk <= 100:
        return None, "ipRisk must be between 0 and 100"
    schema_version = _int_or_none(event.get("schemaVersion", 1))
    if schema_version is None or schema_version < 1:
        return None, "schemaVersion must be a positive integer"

    return event, None


def to_dlq_sample(
    record: dict[str, Any],
    *,
    current_time_millis: int | None = None,
    max_future_skew_millis: int = DEFAULT_MAX_FUTURE_SKEW_MILLIS,
) -> dict[str, Any]:
    value = record.get("value")
    if not isinstance(value, dict):
        value = {}

    raw_value_value = value.get("rawValue")
    raw_value = raw_value_value if isinstance(raw_value_value, str) else ""
    error_type_value = value.get("errorType")
    error_type = (
        error_type_value.strip()
        if isinstance(error_type_value, str) and error_type_value.strip()
        else "UNKNOWN"
    )
    reason_value = value.get("reason")
    reason = (
        reason_value.strip()
        if isinstance(reason_value, str) and reason_value.strip()
        else "UNKNOWN"
    )
    block_reason = replay_block_reason(
        value,
        current_time_millis=current_time_millis,
        max_future_skew_millis=max_future_skew_millis,
    )
    replayable = block_reason is None

    return {
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "key": record.get("key"),
        "errorType": error_type,
        "reason": reason,
        "sourceTopic": value.get("sourceTopic"),
        "replayTopic": value.get("replayTopic"),
        "observedAt": value.get("observedAt"),
        "replayable": replayable,
        "replayBlockReason": block_reason,
        "rawValuePreview": raw_value[:240],
    }


def summarize_dlq_records(
    records: list[dict[str, Any]],
    sample_limit: int = 10,
    *,
    current_time_millis: int | None = None,
    max_future_skew_millis: int = DEFAULT_MAX_FUTURE_SKEW_MILLIS,
) -> dict[str, Any]:
    if sample_limit < 0:
        raise ValueError("sample limit must not be negative")
    evaluation_time = now_millis() if current_time_millis is None else current_time_millis
    by_error_type: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    replayable_by_error_type: defaultdict[str, int] = defaultdict(int)
    samples: list[dict[str, Any]] = []
    replayable_count = 0

    for record in records:
        sample = to_dlq_sample(
            record,
            current_time_millis=evaluation_time,
            max_future_skew_millis=max_future_skew_millis,
        )
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
