import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "transactions.raw")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "20"))
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "60"))
INCLUDE_BAD_EVENTS = os.getenv("INCLUDE_BAD_EVENTS", "true").lower() == "true"

CATEGORIES = ["electronics", "grocery", "travel", "gaming", "fashion", "subscription"]
COUNTRIES = ["KR", "US", "JP", "SG", "DE"]
CHANNELS = ["web", "mobile", "partner_api"]
PAYMENT_STATUSES = ["APPROVED", "APPROVED", "APPROVED", "FAILED"]


def now_millis() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def make_event(index: int) -> dict:
    burst_user = "user-burst" if index % 17 in (0, 1, 2, 3, 4, 5) else None
    hot_merchant = "merchant-hot" if index % 23 in range(0, 12) else None
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


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}", flush=True)


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "realtime-lab-generator",
            "acks": "all",
        }
    )

    total = EVENTS_PER_SECOND * RUN_SECONDS
    delay = 1 / max(EVENTS_PER_SECOND, 1)
    print(f"producing {total} events to {RAW_TOPIC} through {BOOTSTRAP_SERVERS}", flush=True)

    for index in range(total):
        if INCLUDE_BAD_EVENTS and index > 0 and index % 137 == 0:
            payload = '{"eventId": "", "broken": true'
            key = "bad-event"
        else:
            event = make_event(index)
            payload = json.dumps(event, separators=(",", ":"))
            key = event["userId"]

        producer.produce(RAW_TOPIC, key=key, value=payload, callback=delivery_report)
        producer.poll(0)
        time.sleep(delay)

    producer.flush()
    print("done", flush=True)


if __name__ == "__main__":
    main()
