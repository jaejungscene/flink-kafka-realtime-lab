import json
import os
import time

from confluent_kafka import Consumer, KafkaException, Producer
from realtime_lab.dlq_tools import normalize_for_replay, replay_block_reason


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = os.getenv("REPLAY_TOPIC", "transactions.replay")
KAFKA_ISOLATION_LEVEL = os.getenv("KAFKA_ISOLATION_LEVEL", "read_committed")
try:
    MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "50"))
except ValueError as exc:
    raise RuntimeError("MAX_MESSAGES must be an integer") from exc
REPLAYER_GROUP_ID = os.getenv("REPLAYER_GROUP_ID", "realtime-lab-replayer")
REPLAY_RUN_ID = os.getenv("REPLAY_RUN_ID")


def main() -> None:
    if not REPLAY_RUN_ID:
        raise RuntimeError("REPLAY_RUN_ID is required so retries keep stable replay identifiers")
    if MAX_MESSAGES < 1:
        raise RuntimeError("MAX_MESSAGES must be greater than 0")
    if KAFKA_ISOLATION_LEVEL not in {"read_committed", "read_uncommitted"}:
        raise RuntimeError("KAFKA_ISOLATION_LEVEL must be read_committed or read_uncommitted")
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
    delivery_errors: list[str] = []
    deadline = time.time() + 20
    completed = False

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    try:
        while replayed < MAX_MESSAGES and time.time() < deadline:
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

            event = normalize_for_replay(
                dlq_value,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                REPLAY_RUN_ID,
            )
            if not event:
                reason = replay_block_reason(dlq_value) or "unknown replay policy failure"
                print(
                    "skip non-replayable record "
                    f"partition={msg.partition()} offset={msg.offset()}: {reason}",
                    flush=True,
                )
                continue

            producer.produce(
                REPLAY_TOPIC,
                key=event["userId"],
                value=json.dumps(event, separators=(",", ":")),
                callback=delivery_report,
            )
            producer.poll(0)
            replayed += 1
        completed = True
    finally:
        undelivered = producer.flush(10)
        if undelivered or delivery_errors:
            consumer.close()
            raise RuntimeError(
                f"replay delivery failed: undelivered={undelivered}, errors={delivery_errors[:3]}"
            )
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
