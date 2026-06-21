# Replay Guide

The lab separates bad or late events into `transactions.dlq` and provides a small replayer that publishes corrected records into `transactions.replay`.

## Why Replay Topic

Replaying directly into the raw topic makes lineage harder to explain. A dedicated replay topic lets the Flink job consume recovered records while preserving that the event came from remediation.

## Local Flow

```bash
make produce
make consume-dlq
make replay-dlq
make consume-replay
```

The Flink job consumes both `transactions.raw` and `transactions.replay`.

## What The Replayer Fixes

- Missing `eventId`
- Missing user, merchant, category, country, channel, device fields
- Negative amount
- Old event time

The replayer does not try to fix unparseable JSON. That is intentional: in production, some DLQ records require human or batch remediation.

## Production Pattern

- Keep DLQ immutable.
- Write corrected records to a replay topic.
- Add metadata such as operator, ticket id, source DLQ offset, and replay timestamp.
- Protect replay tooling with access control and audit logs.
