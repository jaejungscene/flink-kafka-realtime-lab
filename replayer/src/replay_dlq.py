import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = os.getenv("REPLAY_TOPIC", "transactions.replay")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "50"))


def now_millis() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def normalize_for_replay(dlq_value: dict[str, Any]) -> dict[str, Any] | None:
    raw_value = dlq_value.get("rawValue")
    if not raw_value:
        return None

    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        return None

    event["eventId"] = event.get("eventId") or f"replay-{uuid.uuid4()}"
    event["userId"] = event.get("userId") or "user-replayed"
    event["merchantId"] = event.get("merchantId") or "merchant-replayed"
    event["category"] = event.get("category") or "replay"
    event["eventTime"] = now_millis()
    event["amount"] = max(float(event.get("amount", 0.0)), 0.0)
    event["currency"] = event.get("currency") or "USD"
    event["country"] = event.get("country") or "UNKNOWN"
    event["channel"] = event.get("channel") or "replay"
    event["deviceId"] = event.get("deviceId") or "device-replayed"
    event["mlFraudScore"] = float(event.get("mlFraudScore", 0.0))
    event["paymentStatus"] = event.get("paymentStatus") or "REPLAYED"
    event["ipRisk"] = int(event.get("ipRisk", 0))
    event["replayedFromDlqAt"] = now_millis()
    return event


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"realtime-lab-replayer-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "client.id": "realtime-lab-replayer"})

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

            event = normalize_for_replay(dlq_value)
            if not event:
                continue

            producer.produce(
                REPLAY_TOPIC,
                key=event["userId"],
                value=json.dumps(event, separators=(",", ":")),
            )
            producer.poll(0)
            replayed += 1
    finally:
        producer.flush()
        consumer.close()

    print(f"replayed={replayed} from={DLQ_TOPIC} to={REPLAY_TOPIC}", flush=True)


if __name__ == "__main__":
    main()
