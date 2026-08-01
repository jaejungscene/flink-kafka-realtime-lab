import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer


def non_blank_setting(name: str, fallback: str) -> str:
    value = os.getenv(name, fallback).strip()
    if not value:
        raise RuntimeError(f"{name} must not be blank")
    return value


def positive_int_setting(name: str, fallback: int) -> int:
    raw_value = os.getenv(name)
    try:
        value = fallback if raw_value is None or not raw_value.strip() else int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer: {raw_value}") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be greater than 0: {value}")
    return value


def boolean_setting(name: str, fallback: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return fallback
    normalized = raw_value.strip().lower()
    if normalized not in {"true", "false"}:
        raise RuntimeError(f"{name} must be true or false: {raw_value}")
    return normalized == "true"


BOOTSTRAP_SERVERS = non_blank_setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RAW_TOPIC = non_blank_setting("RAW_TOPIC", "transactions.raw")
EVENTS_PER_SECOND = positive_int_setting("EVENTS_PER_SECOND", 20)
RUN_SECONDS = positive_int_setting("RUN_SECONDS", 60)
INCLUDE_BAD_EVENTS = boolean_setting("INCLUDE_BAD_EVENTS", True)

CATEGORIES = ("electronics", "grocery", "travel", "gaming", "fashion", "subscription")
COUNTRIES = ("KR", "US", "JP", "SG", "DE")
CHANNELS = ("web", "mobile", "partner_api")
PAYMENT_STATUSES = ("APPROVED", "APPROVED", "APPROVED", "FAILED")


def now_millis() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def make_event(index: int) -> dict:
    burst_user = "user-burst" if index % 17 in (0, 1, 2, 3, 4, 5) else None
    hot_merchant = "merchant-hot" if index % 23 in range(12) else None
    risky = random.random() < 0.08

    amount = round(random.uniform(5, 250), 2)
    fraud_score = round(random.uniform(0.01, 0.55), 4)
    ip_risk = random.randint(1, 55)
    payment_status = random.choice(PAYMENT_STATUSES)

    if risky:
        amount = round(random.uniform(800, 2_500), 2)
        fraud_score = round(random.uniform(0.86, 0.99), 4)
        ip_risk = random.randint(75, 100)
        payment_status = random.choice(["FAILED", "APPROVED"])

    if burst_user:
        amount = round(random.uniform(300, 900), 2)
        fraud_score = round(random.uniform(0.35, 0.8), 4)

    if hot_merchant:
        amount = round(random.uniform(500, 1_200), 2)
        fraud_score = round(random.uniform(0.65, 0.95), 4)
        ip_risk = random.randint(60, 95)

    event_time = now_millis() - random.randint(0, 8_000)
    if INCLUDE_BAD_EVENTS and index > 0 and index % 149 == 0:
        event_time = now_millis() - 180_000

    return {
        "schemaVersion": 1,
        "eventId": str(uuid.uuid4()),
        "userId": burst_user or f"user-{random.randint(1, 120):03d}",
        "merchantId": hot_merchant or f"merchant-{random.randint(1, 30):02d}",
        "category": random.choice(CATEGORIES),
        "eventTime": event_time,
        "amount": amount,
        "currency": "USD",
        "country": random.choice(COUNTRIES),
        "channel": random.choice(CHANNELS),
        "deviceId": f"device-{random.randint(1, 300):03d}",
        "mlFraudScore": fraud_score,
        "paymentStatus": payment_status,
        "ipRisk": ip_risk,
    }


def main() -> None:
    delivery_errors: list[str] = []

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "realtime-lab-generator",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    total = EVENTS_PER_SECOND * RUN_SECONDS
    delay = 1 / max(EVENTS_PER_SECOND, 1)
    print(f"producing {total} events to {RAW_TOPIC} through {BOOTSTRAP_SERVERS}", flush=True)

    for index in range(total):
        if INCLUDE_BAD_EVENTS and index > 0 and index % 113 == 0:
            event = make_event(index)
            event["eventId"] = ""
            payload = json.dumps(event, separators=(",", ":"))
            key = event["userId"]
        elif INCLUDE_BAD_EVENTS and index > 0 and index % 137 == 0:
            payload = '{"eventId": "", "broken": true'
            key = "bad-event"
        else:
            event = make_event(index)
            payload = json.dumps(event, separators=(",", ":"))
            key = event["userId"]

        producer.produce(RAW_TOPIC, key=key, value=payload, callback=delivery_report)
        producer.poll(0)
        time.sleep(delay)

    undelivered = producer.flush(10)
    if undelivered or delivery_errors:
        raise RuntimeError(
            f"generator delivery failed: undelivered={undelivered}, errors={delivery_errors[:3]}"
        )
    print("done", flush=True)


if __name__ == "__main__":
    main()
