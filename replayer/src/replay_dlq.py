import json
import os
import time
import uuid

from confluent_kafka import Consumer, Producer
from realtime_lab.dlq_tools import normalize_for_replay


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = os.getenv("REPLAY_TOPIC", "transactions.replay")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "50"))
REPLAYER_GROUP_ID = os.getenv("REPLAYER_GROUP_ID", "realtime-lab-replayer")
REPLAY_RUN_ID = os.getenv("REPLAY_RUN_ID", f"replay-run-{uuid.uuid4()}")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": REPLAYER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "realtime-lab-replayer",
            "acks": "all",
        }
    )

    consumer.subscribe([DLQ_TOPIC])
    replayed = 0
    deadline = time.time() + 20

    try:
        while replayed < MAX_MESSAGES and time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                print(f"skip errored message: {msg.error()}", flush=True)
                continue

            try:
                dlq_value = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:
                print(f"skip invalid dlq json: {exc}", flush=True)
                continue

            event = normalize_for_replay(dlq_value, msg.topic(), msg.partition(), msg.offset(), REPLAY_RUN_ID)
            if not event:
                continue

            producer.produce(
                REPLAY_TOPIC,
                key=event["userId"],
                value=json.dumps(event, separators=(",", ":")),
            )
            producer.poll(0)
            consumer.commit(message=msg, asynchronous=False)
            replayed += 1
    finally:
        producer.flush()
        consumer.close()

    print(
        f"replayed={replayed} from={DLQ_TOPIC} to={REPLAY_TOPIC} "
        f"group={REPLAYER_GROUP_ID} runId={REPLAY_RUN_ID}",
        flush=True,
    )


if __name__ == "__main__":
    main()
