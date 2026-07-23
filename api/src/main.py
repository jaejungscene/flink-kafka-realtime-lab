from __future__ import annotations

import base64
import json
import math
import os
import secrets
import threading
import time
import uuid
from typing import Annotated, Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from realtime_lab.dlq_tools import normalize_for_replay, summarize_dlq_records, to_dlq_sample


def _non_negative_float_setting(name: str, fallback: float) -> float:
    raw_value = os.getenv(name)
    try:
        value = fallback if raw_value is None or not raw_value.strip() else float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number: {raw_value}") from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{name} must be a finite non-negative number: {value}")
    return value


def _topic_list_setting(name: str, fallback: str) -> tuple[str, ...]:
    topics = tuple(topic.strip() for topic in os.getenv(name, fallback).split(",") if topic.strip())
    if not topics:
        raise RuntimeError(f"{name} must contain at least one topic")
    if len(set(topics)) != len(topics):
        raise RuntimeError(f"{name} must not contain duplicate topics")
    return topics


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092").strip()
KAFKA_ISOLATION_LEVEL = os.getenv("KAFKA_ISOLATION_LEVEL", "read_uncommitted")
METRIC_TOPICS = _topic_list_setting(
    "METRIC_TOPICS",
    "transactions.raw,transactions.replay,transactions.aggregates,alerts.fraud,"
    "transactions.dlq,merchant_risk_profiles",
)
FLINK_CONSUMER_GROUP = os.getenv("FLINK_CONSUMER_GROUP", "flink-realtime-lab")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions.dlq")
REPLAY_TOPIC = os.getenv("REPLAY_TOPIC", "transactions.replay")
READABLE_TOPICS = frozenset(_topic_list_setting("READABLE_TOPICS", ",".join(METRIC_TOPICS)))
METRICS_CACHE_SECONDS = _non_negative_float_setting("METRICS_CACHE_SECONDS", 10.0)
API_TOKEN = os.getenv("API_TOKEN", "")

if not BOOTSTRAP_SERVERS:
    raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS must not be blank")
if KAFKA_ISOLATION_LEVEL not in {"read_committed", "read_uncommitted"}:
    raise RuntimeError("KAFKA_ISOLATION_LEVEL must be read_committed or read_uncommitted")
if DLQ_TOPIC == REPLAY_TOPIC:
    raise RuntimeError("DLQ_TOPIC and REPLAY_TOPIC must be different")

_metrics_lock = threading.Lock()
_metrics_cache: tuple[float, str] | None = None

app = FastAPI(title="Flink KRaft Realtime Lab API", version="1.0.0")


class DlqRecordRef(BaseModel):
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class DlqReplayRequest(BaseModel):
    max_messages: int = Field(default=20, ge=1, le=200)
    scan_limit: int = Field(default=200, ge=1, le=1000)
    timeout_seconds: float = Field(default=8.0, ge=1.0, le=30.0)
    from_beginning: bool = True
    dry_run: bool = True
    confirm: bool = False
    replay_run_id: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{2,79}$",
    )
    records: list[DlqRecordRef] = Field(default_factory=list, max_length=200)


def _require_api_token(
    x_api_token: Annotated[str | None, Header(alias="X-API-Token")] = None,
) -> None:
    if API_TOKEN and not secrets.compare_digest(x_api_token or "", API_TOKEN):
        raise HTTPException(status_code=401, detail="A valid X-API-Token header is required")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=3)
    except KafkaException as exc:
        raise HTTPException(status_code=503, detail="Kafka is not reachable") from exc
    return {"status": "ready"}


@app.get("/topics", dependencies=[Depends(_require_api_token)])
def topics() -> dict[str, list[str]]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    return {"topics": sorted(READABLE_TOPICS.intersection(metadata.topics.keys()))}


@app.get("/metrics")
def metrics() -> Response:
    global _metrics_cache

    now = time.monotonic()
    with _metrics_lock:
        if _metrics_cache is None or now - _metrics_cache[0] >= METRICS_CACHE_SECONDS:
            _metrics_cache = (now, _collect_kafka_metrics())

        payload = _metrics_cache[1]

    return Response(
        payload,
        media_type="text/plain; version=0.0.4",
        headers={"Cache-Control": f"public, max-age={int(METRICS_CACHE_SECONDS)}"},
    )


def _collect_kafka_metrics() -> str:
    lines = [
        "# HELP realtime_lab_up API process health indicator.",
        "# TYPE realtime_lab_up gauge",
        "realtime_lab_up 1",
        "# HELP realtime_lab_kafka_up Kafka metadata and offset collection status.",
        "# TYPE realtime_lab_kafka_up gauge",
        "# HELP realtime_lab_kafka_topic_available Whether the configured topic exists.",
        "# TYPE realtime_lab_kafka_topic_available gauge",
        "# HELP realtime_lab_kafka_topic_retained_records Approximate retained records "
        "per topic partition.",
        "# TYPE realtime_lab_kafka_topic_retained_records gauge",
        "# HELP realtime_lab_kafka_topic_log_end_offset Kafka log end offset per topic partition.",
        "# TYPE realtime_lab_kafka_topic_log_end_offset gauge",
        "# HELP realtime_lab_kafka_consumer_lag Consumer group lag by topic partition.",
        "# TYPE realtime_lab_kafka_consumer_lag gauge",
        "# HELP realtime_lab_metrics_partition_errors Partitions whose offsets could not "
        "be collected.",
        "# TYPE realtime_lab_metrics_partition_errors gauge",
    ]

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        metadata = admin.list_topics(timeout=5)
    except KafkaException:
        lines.extend(
            [
                "realtime_lab_kafka_up 0",
                "realtime_lab_metrics_partition_errors 0",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.append("realtime_lab_kafka_up 1")
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": FLINK_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "isolation.level": KAFKA_ISOLATION_LEVEL,
        }
    )
    partition_errors = 0

    try:
        for topic in METRIC_TOPICS:
            topic_label = _prometheus_label(topic)
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                lines.append(f'realtime_lab_kafka_topic_available{{topic="{topic_label}"}} 0')
                continue

            lines.append(f'realtime_lab_kafka_topic_available{{topic="{topic_label}"}} 1')
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
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic, partition), timeout=5
                    )
                except KafkaException:
                    partition_errors += 1
                    continue

                message_count = max(high - low, 0)
                topic_total += message_count
                labels = f'topic="{topic_label}",partition="{partition}"'
                lines.append(
                    f"realtime_lab_kafka_topic_retained_records{{{labels}}} {message_count}"
                )
                lines.append(f"realtime_lab_kafka_topic_log_end_offset{{{labels}}} {high}")

                committed_offset = committed.get(partition)
                if committed_offset is not None:
                    lag = max(high - committed_offset, 0)
                    group_labels = (
                        f'group="{_prometheus_label(FLINK_CONSUMER_GROUP)}",'
                        f'topic="{topic_label}",partition="{partition}"'
                    )
                    lines.append(f"realtime_lab_kafka_consumer_lag{{{group_labels}}} {lag}")

            lines.append(
                f'realtime_lab_kafka_topic_retained_records_total{{topic="{topic_label}"}} '
                f"{topic_total}"
            )
    finally:
        consumer.close()

    lines.append(f"realtime_lab_metrics_partition_errors {partition_errors}")
    return "\n".join(lines) + "\n"


def _prometheus_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


@app.get("/topics/{topic}/messages", dependencies=[Depends(_require_api_token)])
def read_messages(
    topic: str,
    limit: int = 20,
    timeout_seconds: float = 4.0,
    from_beginning: bool = False,
) -> dict[str, Any]:
    _ensure_readable_topic(topic)
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

            messages.append(_message_to_dict(msg))

        return {"topic": topic, "count": len(messages), "messages": messages}
    finally:
        consumer.close()


def read_records_at_offsets(
    topic: str,
    records: list[DlqRecordRef],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    _ensure_readable_topic(topic)
    requested = {(record.partition, record.offset) for record in records}
    if len(requested) != len(records):
        raise HTTPException(
            status_code=400,
            detail="records must not contain duplicate partition/offset pairs",
        )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"realtime-lab-api-selection-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "isolation.level": KAFKA_ISOLATION_LEVEL,
        }
    )
    try:
        first_offsets: dict[int, int] = {}
        for partition, offset in requested:
            topic_partition = TopicPartition(topic, partition)
            low, high = consumer.get_watermark_offsets(topic_partition, timeout=5)
            if offset < low or offset >= high:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"offset is outside the retained range: {topic}[{partition}]@{offset} "
                        f"({low}..{high - 1})"
                    ),
                )
            first_offsets[partition] = min(first_offsets.get(partition, offset), offset)

        consumer.assign(
            [
                TopicPartition(topic, partition, offset)
                for partition, offset in first_offsets.items()
            ]
        )
        deadline = time.time() + timeout_seconds
        found: dict[tuple[int, int], dict[str, Any]] = {}
        while len(found) < len(requested) and time.time() < deadline:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            key = (msg.partition(), msg.offset())
            if key in requested:
                found[key] = _message_to_dict(msg)

        missing = sorted(requested - set(found))
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"selected DLQ records were not readable: {missing}",
            )
        return [found[(record.partition, record.offset)] for record in records]
    finally:
        consumer.close()


def _message_to_dict(message: Any) -> dict[str, Any]:
    raw_bytes = message.value()
    value_encoding = "utf-8"
    if raw_bytes is None:
        value: Any = None
    else:
        try:
            raw_value = raw_bytes.decode("utf-8")
            try:
                value = json.loads(raw_value)
            except json.JSONDecodeError:
                value = raw_value
        except UnicodeDecodeError:
            value = base64.b64encode(raw_bytes).decode("ascii")
            value_encoding = "base64"

    raw_key = message.key()
    key_encoding = "utf-8"
    try:
        key = raw_key.decode("utf-8") if raw_key is not None else None
    except UnicodeDecodeError:
        key = base64.b64encode(raw_key).decode("ascii")
        key_encoding = "base64"
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": key,
        "keyEncoding": key_encoding,
        "value": value,
        "valueEncoding": value_encoding,
    }


def _ensure_readable_topic(topic: str) -> None:
    if topic not in READABLE_TOPICS:
        raise HTTPException(status_code=404, detail="topic is not exposed by this API")


@app.get("/dlq/summary", dependencies=[Depends(_require_api_token)])
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


@app.post("/dlq/replay", dependencies=[Depends(_require_api_token)])
def replay_dlq(request: DlqReplayRequest) -> dict[str, Any]:
    if not request.dry_run:
        if not request.confirm:
            raise HTTPException(
                status_code=400,
                detail="confirm=true is required to publish replay records",
            )
        if not request.replay_run_id:
            raise HTTPException(
                status_code=400,
                detail="replay_run_id is required for idempotent execution",
            )
        if not request.records:
            raise HTTPException(
                status_code=400,
                detail="selected records from a preview are required for execution",
            )

    if request.records:
        messages = read_records_at_offsets(DLQ_TOPIC, request.records, request.timeout_seconds)
    else:
        scan_limit = max(request.scan_limit, request.max_messages)
        messages = read_messages(
            DLQ_TOPIC,
            limit=scan_limit,
            timeout_seconds=request.timeout_seconds,
            from_beginning=request.from_beginning,
        )["messages"]

    replay_run_id = request.replay_run_id or f"api-preview-{uuid.uuid4()}"
    producer = None
    delivery_errors: list[str] = []
    replayed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    if not request.dry_run:
        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "client.id": "realtime-lab-api-replayer",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for record in messages:
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
                callback=delivery_report,
            )
            producer.poll(0)

        if len(replayed) >= request.max_messages:
            break

    if producer is not None:
        undelivered = producer.flush(10)
        if undelivered or delivery_errors:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Kafka replay delivery failed: undelivered={undelivered}, "
                    f"errors={delivery_errors[:3]}"
                ),
            )

    return {
        "runId": replay_run_id,
        "dryRun": request.dry_run,
        "sourceTopic": DLQ_TOPIC,
        "replayTopic": REPLAY_TOPIC,
        "scanned": len(messages),
        "replayed": len(replayed),
        "skipped": len(skipped),
        "records": replayed,
        "skippedSamples": skipped[:10],
    }
