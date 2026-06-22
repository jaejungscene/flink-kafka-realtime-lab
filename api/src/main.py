import json
import os
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException, Response


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
METRIC_TOPICS = [
    topic.strip()
    for topic in os.getenv(
        "METRIC_TOPICS",
        "transactions.raw,transactions.replay,transactions.aggregates,alerts.fraud,transactions.dlq,merchant_risk_profiles",
    ).split(",")
    if topic.strip()
]
FLINK_CONSUMER_GROUP = os.getenv("FLINK_CONSUMER_GROUP", "flink-realtime-lab")

app = FastAPI(title="Flink KRaft Realtime Lab API", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/topics")
def topics() -> dict[str, list[str]]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    return {"topics": sorted(metadata.topics.keys())}


@app.get("/metrics")
def metrics() -> Response:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": FLINK_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    lines = [
        "# HELP realtime_lab_up API health indicator.",
        "# TYPE realtime_lab_up gauge",
        "realtime_lab_up 1",
        "# HELP realtime_lab_kafka_topic_messages Approximate messages per topic partition.",
        "# TYPE realtime_lab_kafka_topic_messages gauge",
        "# HELP realtime_lab_kafka_consumer_lag Consumer group lag by topic partition.",
        "# TYPE realtime_lab_kafka_consumer_lag gauge",
    ]

    try:
        for topic in METRIC_TOPICS:
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                lines.append(f'realtime_lab_kafka_topic_available{{topic="{topic}"}} 0')
                continue

            lines.append(f'realtime_lab_kafka_topic_available{{topic="{topic}"}} 1')
            partitions = sorted(topic_meta.partitions.keys())
            topic_total = 0
            topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
            try:
                committed = {
                    item.partition: item.offset
                    for item in consumer.committed(topic_partitions, timeout=5)
                    if item.offset is not None and item.offset >= 0
                }
            except KafkaException:
                committed = {}

            for partition in partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
                message_count = max(high - low, 0)
                topic_total += message_count
                labels = f'topic="{topic}",partition="{partition}"'
                lines.append(f"realtime_lab_kafka_topic_messages{{{labels}}} {message_count}")
                lines.append(f"realtime_lab_kafka_topic_log_end_offset{{{labels}}} {high}")

                committed_offset = committed.get(partition)
                if committed_offset is not None:
                    lag = max(high - committed_offset, 0)
                    group_labels = f'group="{FLINK_CONSUMER_GROUP}",topic="{topic}",partition="{partition}"'
                    lines.append(f"realtime_lab_kafka_consumer_lag{{{group_labels}}} {lag}")

            lines.append(f'realtime_lab_kafka_topic_messages_total{{topic="{topic}"}} {topic_total}')
    finally:
        consumer.close()

    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


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
