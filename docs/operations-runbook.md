# Operations Runbook

## Local Startup

```bash
make build
make up
make produce
make smoke
```

## Health Checks

```bash
docker compose ps
curl http://localhost:8081/jobs
curl http://localhost:8000/health
make topics
make lag
```

Expected:

- Kafka is healthy.
- Flink has a running job named `flink-kraft-realtime-lab`.
- `alerts.fraud`, `transactions.aggregates`, and `transactions.dlq` receive messages after the generator runs.

## Topic Checks

```bash
make consume-alerts
make consume-aggregates
make consume-dlq
make replay-dlq
make consume-replay
```

## Common Issues

### Flink job has no output

- Check whether `make produce` ran after the Flink job started.
- Aggregates emit after event-time windows close.
- Source offsets start at `latest`, so old messages are intentionally skipped.

### Consumer lag grows

- Run `make lag`.
- Increase Flink parallelism only after checking Kafka partition count.
- Inspect Flink UI for backpressure and failed checkpoints.

### DLQ has many records

- Check producer schema changes.
- Inspect `reason` and `errorType`.
- Use `make replay-dlq` only for recoverable records.

### Kubernetes custom resources fail

- Confirm Strimzi and Flink Kubernetes Operator CRDs are installed.
- Render manifests first with `kubectl kustomize`.
- Check operator logs before debugging application pods.

## Production Differences

- Use 3+ Kafka brokers/controllers.
- Use durable Flink checkpoint storage.
- Add TLS/authentication and network policies.
- Add Schema Registry with Avro or Protobuf.
- Define replay access control and audit trails.
- Monitor Kafka lag, Flink checkpoint failures, backpressure, restart count, and end-to-end latency.
