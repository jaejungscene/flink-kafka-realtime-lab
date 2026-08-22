from __future__ import annotations

import base64
import json
import logging
import secrets
import threading
import time
import uuid
from typing import Annotated, Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from realtime_lab.dlq_tools import (
    normalize_for_replay,
    summarize_dlq_records,
    to_dlq_sample,
)
from realtime_lab.kafka_delivery import (
    KafkaDeliveryError,
    KafkaPublishRecord,
    publish_and_wait,
)
from src.config import AppSettings
from src.models import DlqRecordRef, DlqReplayRequest

SETTINGS = AppSettings.from_environment()
BOOTSTRAP_SERVERS = SETTINGS.bootstrap_servers
KAFKA_ISOLATION_LEVEL = SETTINGS.kafka_isolation_level
METRIC_TOPICS = SETTINGS.metric_topics
FLINK_CONSUMER_GROUP = SETTINGS.flink_consumer_group
DLQ_TOPIC = SETTINGS.dlq_topic
REPLAY_TOPIC = SETTINGS.replay_topic
READABLE_TOPICS = SETTINGS.readable_topics
METRICS_CACHE_SECONDS = SETTINGS.metrics_cache_seconds
MAX_FUTURE_SKEW_MILLIS = SETTINGS.max_future_skew_millis
API_TOKEN = SETTINGS.api_token

_metrics_lock = threading.Lock()
_metrics_cache: tuple[float, str] | None = None
logger = logging.getLogger(__name__)

app = FastAPI(title="Flink KRaft Realtime Lab API", version="1.0.0")


def _require_api_token(
    x_api_token: Annotated[str | None, Header(alias="X-API-Token")] = None,
) -> None:
    if API_TOKEN and not secrets.compare_digest(x_api_token or "", API_TOKEN):
        raise HTTPException(status_code=401, detail="A valid X-API-Token header is required")


@app.exception_handler(KafkaException)
async def kafka_exception_handler(_request: Request, exception: KafkaException) -> JSONResponse:
    logger.warning("Kafka request failed: %s", exception)
    return JSONResponse(status_code=503, content={"detail": "Kafka request failed"})


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    try:
        metadata = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=3)
    except KafkaException as exc:
        raise HTTPException(status_code=503, detail="Kafka is not reachable") from exc
    unavailable = _unavailable_topics(metadata, READABLE_TOPICS)
    if unavailable:
        logger.warning("Required Kafka topics are unavailable: %s", unavailable)
        raise HTTPException(status_code=503, detail="Required Kafka topics are unavailable")
    return {"status": "ready"}


def _unavailable_topics(metadata: Any, required_topics: set[str] | frozenset[str]) -> list[str]:
    return sorted(
        topic
        for topic in required_topics
        if topic not in metadata.topics or metadata.topics[topic].error is not None
    )


@app.get("/topics", dependencies=[Depends(_require_api_token)])
def topics() -> dict[str, list[str]]:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    available_topics = (
        topic
        for topic in READABLE_TOPICS
        if topic in metadata.topics and metadata.topics[topic].error is None
    )
    return {"topics": sorted(available_topics)}


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

    finally:
        consumer.close()

    lines.append(f"realtime_lab_metrics_partition_errors {partition_errors}")
    return "\n".join(lines) + "\n"


def _prometheus_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


@app.get("/topics/{topic}/messages", dependencies=[Depends(_require_api_token)])
def read_messages(
    topic: str,
    limit: Annotated[int, Query(ge=1, le=1000)] = 20,
    timeout_seconds: Annotated[float, Query(ge=0.1, le=30.0)] = 4.0,
    from_beginning: bool = False,
) -> dict[str, Any]:
    partitions = _topic_partitions(topic)
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
        deadline = time.monotonic() + timeout_seconds
        messages: list[dict[str, Any]] = []

        while len(messages) < limit and time.monotonic() < deadline:
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
    if not records:
        return []

    partitions = set(_topic_partitions(topic))
    missing_partitions = sorted({record.partition for record in records} - partitions)
    if missing_partitions:
        raise HTTPException(
            status_code=404,
            detail=f"topic partitions not found: {missing_partitions}",
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
        retained_ranges: dict[int, tuple[int, int]] = {}
        for partition, offset in requested:
            if partition not in retained_ranges:
                retained_ranges[partition] = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition),
                    timeout=5,
                )
            low, high = retained_ranges[partition]
            if offset < low or offset >= high:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"offset is outside the retained range: {topic}[{partition}]@{offset} "
                        f"({low}..{high - 1})"
                    ),
                )
        deadline = time.monotonic() + timeout_seconds
        found: list[dict[str, Any]] = []
        for record in records:
            consumer.assign([TopicPartition(topic, record.partition, record.offset)])
            while time.monotonic() < deadline:
                msg = consumer.poll(0.2)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                if msg.partition() == record.partition and msg.offset() == record.offset:
                    found.append(_message_to_dict(msg))
                    break
            else:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "selected DLQ record was not readable: "
                        f"{topic}[{record.partition}]@{record.offset}"
                    ),
                )
        return found
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


def _topic_partitions(topic: str) -> list[int]:
    _ensure_readable_topic(topic)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(topic=topic, timeout=5)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise HTTPException(status_code=404, detail=f"topic not found: {topic}")
    return sorted(topic_metadata.partitions)


@app.get("/dlq/summary", dependencies=[Depends(_require_api_token)])
def dlq_summary(
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
    timeout_seconds: Annotated[float, Query(ge=0.1, le=30.0)] = 6.0,
    from_beginning: bool = True,
) -> dict[str, Any]:
    result = read_messages(
        DLQ_TOPIC,
        limit=limit,
        timeout_seconds=timeout_seconds,
        from_beginning=from_beginning,
    )
    summary = summarize_dlq_records(
        result["messages"],
        max_future_skew_millis=MAX_FUTURE_SKEW_MILLIS,
    )
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
                detail="replay_run_id is required to keep retry identifiers stable",
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
    publish_records: list[KafkaPublishRecord] = []
    replayed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    replay_time = int(time.time() * 1000)

    if not request.dry_run:
        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "client.id": "realtime-lab-api-replayer",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

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
            current_time_millis=replay_time,
            max_future_skew_millis=MAX_FUTURE_SKEW_MILLIS,
        )
        if event is None:
            skipped.append(
                to_dlq_sample(
                    record,
                    current_time_millis=replay_time,
                    max_future_skew_millis=MAX_FUTURE_SKEW_MILLIS,
                )
            )
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
            publish_records.append(
                KafkaPublishRecord(
                    topic=REPLAY_TOPIC,
                    key=event["userId"],
                    value=json.dumps(event, separators=(",", ":")),
                )
            )

        if len(replayed) >= request.max_messages:
            break

    if producer is not None:
        try:
            publish_and_wait(producer, publish_records)
        except KafkaDeliveryError:
            logger.exception("Kafka replay delivery failed")
            raise HTTPException(
                status_code=502,
                detail="Kafka replay delivery failed",
            ) from None

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
