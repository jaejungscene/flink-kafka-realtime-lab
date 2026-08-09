from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_METRIC_TOPICS = (
    "transactions.raw,transactions.replay,transactions.aggregates,alerts.fraud,"
    "transactions.dlq,merchant_risk_profiles"
)


def _non_blank(source: Mapping[str, str], name: str, fallback: str) -> str:
    value = source.get(name, fallback).strip()
    if not value:
        raise RuntimeError(f"{name} must not be blank")
    return value


def _non_negative_float(
    source: Mapping[str, str], name: str, fallback: float
) -> float:
    raw_value = source.get(name)
    try:
        value = fallback if raw_value is None or not raw_value.strip() else float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number: {raw_value}") from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{name} must be a finite non-negative number: {value}")
    return value


def _topic_list(source: Mapping[str, str], name: str, fallback: str) -> tuple[str, ...]:
    topics = tuple(
        topic.strip()
        for topic in source.get(name, fallback).split(",")
        if topic.strip()
    )
    if not topics:
        raise RuntimeError(f"{name} must contain at least one topic")
    if len(set(topics)) != len(topics):
        raise RuntimeError(f"{name} must not contain duplicate topics")
    return topics


@dataclass(frozen=True)
class AppSettings:
    bootstrap_servers: str
    kafka_isolation_level: str
    metric_topics: tuple[str, ...]
    flink_consumer_group: str
    dlq_topic: str
    replay_topic: str
    readable_topics: frozenset[str]
    metrics_cache_seconds: float
    max_future_skew_millis: int
    api_token: str

    @classmethod
    def from_environment(
        cls, source: Mapping[str, str] | None = None
    ) -> AppSettings:
        values = os.environ if source is None else source
        metric_topics = _topic_list(values, "METRIC_TOPICS", DEFAULT_METRIC_TOPICS)
        dlq_topic = _non_blank(values, "DLQ_TOPIC", "transactions.dlq")
        replay_topic = _non_blank(values, "REPLAY_TOPIC", "transactions.replay")
        readable_topics = frozenset(
            _topic_list(values, "READABLE_TOPICS", ",".join(metric_topics))
        )
        isolation_level = _non_blank(
            values, "KAFKA_ISOLATION_LEVEL", "read_uncommitted"
        ).lower()

        if isolation_level not in {"read_committed", "read_uncommitted"}:
            raise RuntimeError(
                "KAFKA_ISOLATION_LEVEL must be read_committed or read_uncommitted"
            )
        if dlq_topic == replay_topic:
            raise RuntimeError("DLQ_TOPIC and REPLAY_TOPIC must be different")
        if dlq_topic not in readable_topics:
            raise RuntimeError("DLQ_TOPIC must be included in READABLE_TOPICS")

        return cls(
            bootstrap_servers=_non_blank(
                values, "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
            ),
            kafka_isolation_level=isolation_level,
            metric_topics=metric_topics,
            flink_consumer_group=_non_blank(
                values, "FLINK_CONSUMER_GROUP", "flink-realtime-lab"
            ),
            dlq_topic=dlq_topic,
            replay_topic=replay_topic,
            readable_topics=readable_topics,
            metrics_cache_seconds=_non_negative_float(
                values, "METRICS_CACHE_SECONDS", 10.0
            ),
            max_future_skew_millis=int(
                _non_negative_float(values, "MAX_FUTURE_SKEW_SECONDS", 300.0) * 1000
            ),
            api_token=values.get("API_TOKEN", "").strip(),
        )
