import json
import os
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from src.dlq_tools import normalize_for_replay, summarize_dlq_records, to_dlq_sample


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_ISOLATION_LEVEL = os.getenv("KAFKA_ISOLATION_LEVEL", "read_uncommitted")
METRIC_TOPICS = [
    topic.strip()
    for topic in os.getenv(
        "METRIC_TOPICS",
        "transactions.raw,transactions.replay,transactions.aggregates,alerts.fraud,transactions.dlq,merchant_risk_profiles",
    ).split(",")
    if topic.strip()
]
FLINK_CONSUMER_GROUP = os.getenv("FLINK_CONSUMER_GROUP", "flink-realtime-lab")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = os.getenv("REPLAY_TOPIC", "transactions.replay")

app = FastAPI(title="Flink KRaft Realtime Lab API", version="1.0.0")


class DlqReplayRequest(BaseModel):
    max_messages: int = Field(default=20, ge=1, le=200)
    scan_limit: int = Field(default=200, ge=1, le=1000)
    timeout_seconds: float = Field(default=8.0, ge=1.0, le=30.0)
    from_beginning: bool = True
    dry_run: bool = True


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
            "isolation.level": KAFKA_ISOLATION_LEVEL,
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
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 1000")

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
            "isolation.level": KAFKA_ISOLATION_LEVEL,
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


@app.get("/dlq/summary")
def dlq_summary(
    limit: int = 200,
    timeout_seconds: float = 6.0,
    from_beginning: bool = True,
) -> dict[str, Any]:
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 1000")

    result = read_messages(
        DLQ_TOPIC,
        limit=limit,
        timeout_seconds=timeout_seconds,
        from_beginning=from_beginning,
    )
    summary = summarize_dlq_records(result["messages"])
    return {
        "topic": DLQ_TOPIC,
        "scanLimit": limit,
        "fromBeginning": from_beginning,
        **summary,
    }


@app.post("/dlq/replay")
def replay_dlq(request: DlqReplayRequest) -> dict[str, Any]:
    scan_limit = max(request.scan_limit, request.max_messages)
    result = read_messages(
        DLQ_TOPIC,
        limit=scan_limit,
        timeout_seconds=request.timeout_seconds,
        from_beginning=request.from_beginning,
    )
    replay_run_id = f"api-replay-{uuid.uuid4()}"
    producer = None
    replayed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    if not request.dry_run:
        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "client.id": "realtime-lab-api-replayer",
                "acks": "all",
            }
        )

    try:
        for record in result["messages"]:
            value = record.get("value")
            if not isinstance(value, dict):
                skipped.append(
                    {
                        "partition": record.get("partition"),
                        "offset": record.get("offset"),
                        "reason": "DLQ value is not a JSON object",
                    }
                )
                continue

            event = normalize_for_replay(
                value,
                str(record.get("topic") or DLQ_TOPIC),
                int(record.get("partition") or 0),
                int(record.get("offset") or 0),
                replay_run_id,
            )
            if event is None:
                skipped.append(to_dlq_sample(record, replay_run_id))
                continue

            replayed.append(
                {
                    "partition": record.get("partition"),
                    "offset": record.get("offset"),
                    "eventId": event["eventId"],
                    "userId": event["userId"],
                    "replayId": event["replayId"],
                }
            )

            if producer is not None:
                producer.produce(
                    REPLAY_TOPIC,
                    key=event["userId"],
                    value=json.dumps(event, separators=(",", ":")),
                )
                producer.poll(0)

            if len(replayed) >= request.max_messages:
                break
    finally:
        if producer is not None:
            producer.flush()

    return {
        "runId": replay_run_id,
        "dryRun": request.dry_run,
        "sourceTopic": DLQ_TOPIC,
        "replayTopic": REPLAY_TOPIC,
        "scanned": len(result["messages"]),
        "replayed": len(replayed),
        "skipped": len(skipped),
        "records": replayed,
        "skippedSamples": skipped[:10],
    }
