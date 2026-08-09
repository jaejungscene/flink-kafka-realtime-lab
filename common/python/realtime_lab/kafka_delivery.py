from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class KafkaPublishRecord:
    topic: str
    key: str
    value: str


class KafkaDeliveryError(RuntimeError):
    """Raised when Kafka does not acknowledge every requested record."""


def publish_and_wait(
    producer: Any,
    records: Iterable[KafkaPublishRecord],
    timeout_seconds: float = 10.0,
) -> int:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")

    deadline = time.monotonic() + timeout_seconds
    delivery_errors: list[str] = []
    published = 0

    def delivery_report(error: Any, _message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for record in records:
        while True:
            try:
                producer.produce(
                    record.topic,
                    key=record.key,
                    value=record.value,
                    callback=delivery_report,
                )
                published += 1
                producer.poll(0)
                break
            except BufferError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise KafkaDeliveryError(
                        f"Kafka producer queue remained full: published={published}"
                    ) from exc
                producer.poll(min(0.1, remaining))

    remaining = max(0.0, deadline - time.monotonic())
    undelivered = producer.flush(remaining)
    if undelivered or delivery_errors:
        raise KafkaDeliveryError(
            "Kafka delivery failed: "
            f"published={published}, undelivered={undelivered}, "
            f"errors={delivery_errors[:3]}"
        )
    return published
