# Learning Guide

## 1. Kafka KRaft

Kafka 4.x is KRaft-only. This lab uses a single local broker/controller in Docker Compose and a Strimzi cluster in Kubernetes so learners can compare local development with operator-managed deployment.

Key ideas:

- Topic creation is explicit.
- Partition count is part of system design.
- Consumer lag is an operational signal, not just a Kafka detail.
- Replay is separated from raw ingestion for lineage.

## 2. Flink Event Time

Flink processes `eventTime`, not the time a container happened to receive the record.

```java
WatermarkStrategy
    .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
```

The job also uses `allowedLateness(Time.seconds(30))` and sends events that arrive after the allowed lateness to DLQ-like handling.

## 3. Alerting Patterns

The project includes three common alert patterns:

- Single-event risk: one transaction is dangerous enough by itself.
- Keyed user burst: a user creates too many or too expensive payments in one minute.
- Merchant anomaly: a merchant shows abnormal volume, amount, or average fraud score.

Rules live in `RiskRules` so they can be tested and changed without reading the full Flink topology.

## 4. Aggregation Pattern

`transactions.aggregates` emits one-minute `country|category|merchant` aggregates:

- event count
- total amount
- average amount
- average fraud score

In production, this stream could feed Redis, ClickHouse, Pinot, Druid, Elasticsearch, BigQuery, or a real-time dashboard.

## 5. DLQ And Replay

The job reads Kafka values as strings first, then parses JSON inside Flink. This makes bad-event handling visible:

- malformed JSON
- missing required fields
- negative amount
- too-late event

Recoverable DLQ records can be normalized by the replayer and published to `transactions.replay`.

## 6. Exercises

1. Change fraud thresholds in `RiskRules` and run `make test`.
2. Increase `EVENTS_PER_SECOND` and observe `make lag`.
3. Change watermark delay and compare aggregate latency.
4. Run `make replay-dlq` and inspect `transactions.replay`.
5. Render both Kubernetes overlays and compare dev vs prod-like operational choices.
