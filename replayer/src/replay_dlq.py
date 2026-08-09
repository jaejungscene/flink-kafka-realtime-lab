import json
import os
import time

from confluent_kafka import Consumer, KafkaException, Producer

from realtime_lab.dlq_tools import (
    normalize_for_replay,
    replay_block_reason,
    validate_replay_run_id,
)
from realtime_lab.kafka_delivery import KafkaPublishRecord, publish_and_wait


def non_blank_setting(name: str, fallback: str) -> str:
    value = os.getenv(name, fallback).strip()
    if not value:
        raise RuntimeError(f"{name} must not be blank")
    return value


def int_setting(name: str, fallback: int) -> int:
    raw_value = os.getenv(name)
    try:
        return fallback if raw_value is None or not raw_value.strip() else int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer: {raw_value}") from exc


BOOTSTRAP_SERVERS = non_blank_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DLQ_TOPIC = non_blank_setting("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = non_blank_setting("REPLAY_TOPIC", "transactions.replay")
KAFKA_ISOLATION_LEVEL = non_blank_setting(
    "KAFKA_ISOLATION_LEVEL", "read_committed"
).lower()
MAX_MESSAGES = int_setting("MAX_MESSAGES", 50)
REPLAYER_GROUP_ID = non_blank_setting("REPLAYER_GROUP_ID", "realtime-lab-replayer")
REPLAY_RUN_ID = os.getenv("REPLAY_RUN_ID")
MAX_FUTURE_SKEW_SECONDS = int_setting("MAX_FUTURE_SKEW_SECONDS", 300)


def main() -> None:
    if not REPLAY_RUN_ID:
        raise RuntimeError("REPLAY_RUN_ID is required so retries keep stable replay identifiers")
    try:
        validate_replay_run_id(REPLAY_RUN_ID)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    if MAX_MESSAGES < 1:
        raise RuntimeError("MAX_MESSAGES must be greater than 0")
    if KAFKA_ISOLATION_LEVEL not in {"read_committed", "read_uncommitted"}:
        raise RuntimeError("KAFKA_ISOLATION_LEVEL must be read_committed or read_uncommitted")
    if MAX_FUTURE_SKEW_SECONDS < 0:
        raise RuntimeError("MAX_FUTURE_SKEW_SECONDS must not be negative")
    if DLQ_TOPIC == REPLAY_TOPIC:
        raise RuntimeError("DLQ_TOPIC and REPLAY_TOPIC must be different")

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": REPLAYER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": KAFKA_ISOLATION_LEVEL,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "realtime-lab-replayer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    consumer.subscribe([DLQ_TOPIC])
    replayed = 0
    consumed = 0
    publish_records: list[KafkaPublishRecord] = []
    deadline = time.monotonic() + 20
    completed = False

    try:
        while replayed < MAX_MESSAGES and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            consumed += 1

            try:
                dlq_value = json.loads(msg.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                print(f"skip invalid dlq json: {exc}", flush=True)
                continue

            replay_time = int(time.time() * 1000)
            event = normalize_for_replay(
                dlq_value,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                REPLAY_RUN_ID,
                current_time_millis=replay_time,
                max_future_skew_millis=MAX_FUTURE_SKEW_SECONDS * 1000,
            )
            if not event:
                reason = replay_block_reason(
                    dlq_value,
                    current_time_millis=replay_time,
                    max_future_skew_millis=MAX_FUTURE_SKEW_SECONDS * 1000,
                ) or "unknown replay policy failure"
                print(
                    "skip non-replayable record "
                    f"partition={msg.partition()} offset={msg.offset()}: {reason}",
                    flush=True,
                )
                continue

            publish_records.append(
                KafkaPublishRecord(
                    topic=REPLAY_TOPIC,
                    key=event["userId"],
                    value=json.dumps(event, separators=(",", ":")),
                )
            )
            replayed += 1
        publish_and_wait(producer, publish_records)
        completed = True
    finally:
        if completed and consumed:
            consumer.commit(asynchronous=False)
        consumer.close()

    print(
        f"replayed={replayed} from={DLQ_TOPIC} to={REPLAY_TOPIC} "
        f"group={REPLAYER_GROUP_ID} runId={REPLAY_RUN_ID}",
        flush=True,
    )


if __name__ == "__main__":
    main()
