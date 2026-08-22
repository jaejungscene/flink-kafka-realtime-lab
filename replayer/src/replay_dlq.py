import json
import time

from confluent_kafka import Consumer, KafkaException, Producer

from realtime_lab.dlq_tools import (
    normalize_for_replay,
    replay_block_reason,
)
from realtime_lab.kafka_delivery import KafkaPublishRecord, publish_and_wait
from src.config import ReplayerSettings


def main(settings: ReplayerSettings | None = None) -> None:
    settings = settings or ReplayerSettings.from_environment()

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": settings.isolation_level,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "realtime-lab-replayer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    consumer.subscribe([settings.dlq_topic])
    replayed = 0
    consumed = 0
    publish_records: list[KafkaPublishRecord] = []
    deadline = time.monotonic() + 20
    completed = False

    try:
        while replayed < settings.max_messages and time.monotonic() < deadline:
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
                settings.replay_run_id,
                current_time_millis=replay_time,
                max_future_skew_millis=settings.max_future_skew_millis,
            )
            if not event:
                reason = replay_block_reason(
                    dlq_value,
                    current_time_millis=replay_time,
                    max_future_skew_millis=settings.max_future_skew_millis,
                ) or "unknown replay policy failure"
                print(
                    "skip non-replayable record "
                    f"partition={msg.partition()} offset={msg.offset()}: {reason}",
                    flush=True,
                )
                continue

            publish_records.append(
                KafkaPublishRecord(
                    topic=settings.replay_topic,
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
        f"replayed={replayed} from={settings.dlq_topic} to={settings.replay_topic} "
        f"group={settings.consumer_group} runId={settings.replay_run_id}",
        flush=True,
    )


if __name__ == "__main__":
    main()
