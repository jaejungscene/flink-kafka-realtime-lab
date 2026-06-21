import json
import os
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

app = FastAPI(title="Flink KRaft Realtime Lab API", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/topics")
def topics() -> dict[str, list[str]]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    return {"topics": sorted(metadata.topics.keys())}


@app.get("/topics/{topic}/messages")
def read_messages(
    topic: str,
    limit: int = 20,
    timeout_seconds: float = 4.0,
    from_beginning: bool = False,
) -> dict[str, Any]:
    if limit < 1 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(topic=topic, timeout=5)
    if topic not in metadata.topics or metadata.topics[topic].error is not None:
        raise HTTPException(status_code=404, detail=f"topic not found: {topic}")

    partitions = list(metadata.topics[topic].partitions.keys())
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"realtime-lab-api-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )

    try:
        offsets = []
        per_partition_limit = max(1, limit // max(len(partitions), 1) + 1)
        for partition in partitions:
            topic_partition = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(topic_partition, timeout=5)
            start_offset = low if from_beginning else max(low, high - per_partition_limit)
            offsets.append(TopicPartition(topic, partition, start_offset))

        consumer.assign(offsets)
        deadline = time.time() + timeout_seconds
        messages: list[dict[str, Any]] = []

        while len(messages) < limit and time.time() < deadline:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            raw_value = msg.value().decode("utf-8") if msg.value() else ""
            try:
                value: Any = json.loads(raw_value)
            except json.JSONDecodeError:
                value = raw_value

            messages.append(
                {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": value,
                }
            )

        return {"topic": topic, "count": len(messages), "messages": messages}
    finally:
        consumer.close()
