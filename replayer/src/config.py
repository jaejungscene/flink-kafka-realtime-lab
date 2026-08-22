from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from realtime_lab.dlq_tools import validate_replay_run_id


def _non_blank(source: Mapping[str, str], name: str, fallback: str | None = None) -> str:
    value = source.get(name, fallback or "").strip()
    if not value:
        raise RuntimeError(f"{name} must not be blank")
    return value


def _integer(source: Mapping[str, str], name: str, fallback: int) -> int:
    raw_value = source.get(name)
    try:
        return fallback if raw_value is None or not raw_value.strip() else int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer: {raw_value}") from exc


@dataclass(frozen=True)
class ReplayerSettings:
    bootstrap_servers: str
    dlq_topic: str
    replay_topic: str
    isolation_level: str
    max_messages: int
    consumer_group: str
    replay_run_id: str
    max_future_skew_millis: int

    @classmethod
    def from_environment(
        cls, source: Mapping[str, str] | None = None
    ) -> ReplayerSettings:
        values = os.environ if source is None else source
        dlq_topic = _non_blank(values, "DLQ_TOPIC", "transactions.dlq")
        replay_topic = _non_blank(values, "REPLAY_TOPIC", "transactions.replay")
        isolation_level = _non_blank(
            values, "KAFKA_ISOLATION_LEVEL", "read_committed"
        ).lower()
        max_messages = _integer(values, "MAX_MESSAGES", 50)
        max_future_skew_seconds = _integer(values, "MAX_FUTURE_SKEW_SECONDS", 300)
        replay_run_id = _non_blank(values, "REPLAY_RUN_ID")

        if dlq_topic == replay_topic:
            raise RuntimeError("DLQ_TOPIC and REPLAY_TOPIC must be different")
        if isolation_level not in {"read_committed", "read_uncommitted"}:
            raise RuntimeError(
                "KAFKA_ISOLATION_LEVEL must be read_committed or read_uncommitted"
            )
        if max_messages < 1:
            raise RuntimeError("MAX_MESSAGES must be greater than 0")
        if max_future_skew_seconds < 0:
            raise RuntimeError("MAX_FUTURE_SKEW_SECONDS must not be negative")
        try:
            validate_replay_run_id(replay_run_id)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        return cls(
            bootstrap_servers=_non_blank(
                values, "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
            ),
            dlq_topic=dlq_topic,
            replay_topic=replay_topic,
            isolation_level=isolation_level,
            max_messages=max_messages,
            consumer_group=_non_blank(
                values, "REPLAYER_GROUP_ID", "realtime-lab-replayer"
            ),
            replay_run_id=replay_run_id,
            max_future_skew_millis=max_future_skew_seconds * 1000,
        )
