# 이벤트 스키마

현재 프로젝트는 JSON을 사용합니다. 실무에서는 Avro 또는 Protobuf와 Schema Registry를 붙이는 구성을 권장합니다.

## 토픽 목록

| Topic | 목적 |
| --- | --- |
| `transactions.raw` | 원천 결제/ML 이벤트 |
| `transactions.replay` | 안전성 검사를 통과한 DLQ 재처리 이벤트 |
| `transactions.aggregates` | Flink 실시간 집계 결과 |
| `transactions.aggregates.sql` | Flink SQL 집계 예제 결과 |
| `alerts.fraud` | Flink 알람 판단 결과 |
| `transactions.dlq` | 파싱/검증/late event 격리 |
| `merchant_risk_profiles` | PostgreSQL CDC 기반 가맹점 risk profile |

## `transactions.raw`와 `transactions.replay`

```json
{
  "schemaVersion": 1,
  "eventId": "8d6296df-8fdf-49fe-87a2-cf9476f54f3d",
  "userId": "user-001",
  "merchantId": "merchant-07",
  "category": "electronics",
  "eventTime": 1760000000000,
  "amount": 129.99,
  "currency": "USD",
  "country": "KR",
  "channel": "mobile",
  "deviceId": "device-010",
  "mlFraudScore": 0.42,
  "paymentStatus": "APPROVED",
  "ipRisk": 35,
  "replayId": "replay-run-local-0-42",
  "replayRunId": "replay-run-local",
  "replaySourceTopic": "transactions.dlq",
  "replaySourcePartition": 0,
  "replaySourceOffset": 42,
  "replayedFromDlqAt": 1760000005000
}
```

필수 field:

- `eventId`
- `userId`
- `eventTime`
- `amount >= 0`
- `schemaVersion >= 1`

`replay*` field는 `transactions.replay`에서만 붙을 수 있는 audit metadata입니다.
자동 replay는 기존 `userId`, `eventTime`, 유효한 숫자 값을 보존하며, 비어 있는
`eventId`만 DLQ source 좌표로 결정적으로 생성할 수 있습니다.

## `alerts.fraud`

`alertType` 예시:

- `HIGH_RISK_TRANSACTION`
- `USER_PAYMENT_BURST`
- `MERCHANT_ANOMALY`

```json
{
  "schemaVersion": 1,
  "alertId": "26a0b4c6-4f02-44e8-88c1-271a203d2a65",
  "alertType": "HIGH_RISK_TRANSACTION",
  "severity": "CRITICAL",
  "key": "user-001",
  "reason": "single event exceeded fraud rule threshold; merchantRiskTier=HIGH, merchantRiskMultiplier=1.700, effectiveFraudScore=0.9800",
  "windowStart": 1760000000000,
  "windowEnd": 1760000000000,
  "eventTime": 1760000000000,
  "metricName": "effectiveFraudScore",
  "metricValue": 0.98,
  "sampleEventId": "8d6296df-8fdf-49fe-87a2-cf9476f54f3d"
}
```

## `transactions.aggregates`

```json
{
  "schemaVersion": 1,
  "aggregateType": "COUNTRY_CATEGORY_MERCHANT_1M",
  "key": "KR|electronics|merchant-07",
  "windowStart": 1760000000000,
  "windowEnd": 1760000060000,
  "eventCount": 42,
  "totalAmount": 8392.12,
  "avgAmount": 199.81,
  "avgFraudScore": 0.23
}
```

## `transactions.dlq`

```json
{
  "schemaVersion": 1,
  "errorType": "PARSE_OR_VALIDATION_ERROR",
  "reason": "eventId is required",
  "sourceTopic": "transactions.raw",
  "sourcePartition": 2,
  "sourceOffset": 42,
  "sourceTimestamp": 1760000000100,
  "sourceKey": "user-001",
  "replayTopic": "transactions.replay",
  "rawValue": "{\"eventId\":\"\"}",
  "observedAt": 1760000000000
}
```

`errorType` 예시:

- `PARSE_OR_VALIDATION_ERROR`
- `LATE_EVENT`
- `REFERENCE_DATA_PARSE_ERROR`

`LATE_EVENT`와 `REFERENCE_DATA_PARSE_ERROR`는 자동 replay 대상이 아닙니다. DLQ source
좌표는 원본 Kafka record를 추적하고 별도 remediation 결과를 감사하는 데 사용합니다.

## `merchant_risk_profiles`

CDC 선택 profile을 실행하면 PostgreSQL의 `merchant_risk_profiles` table 변경이 이 topic으로 발행됩니다.

```json
{
  "merchant_id": "merchant-hot",
  "risk_tier": "HIGH",
  "risk_multiplier": 1.8,
  "manual_review_required": true,
  "updated_at": "2026-06-22T10:00:00Z"
}
```

삭제 event는 Debezium rewrite 형식으로 같은 key와 `"__deleted": "true"`를 포함하며,
Flink는 해당 profile을 Broadcast State에서 제거합니다.

이 topic은 compacted topic입니다. Flink job은 earliest부터 읽어 Broadcast State를 구성하고, transaction의 `merchantId`와 join해 effective fraud score 계산에 사용합니다.

적용 규칙:

- profile 없음: multiplier `1.0`
- profile 있음: `mlFraudScore * risk_multiplier`, 최대 `1.0`
- `manual_review_required=true`: 경계선 이벤트를 알람으로 승격 가능

## Avro schema contract

`schemas/` 디렉터리에는 topic별 Avro schema 예제가 있습니다. 등록 방법은 [Schema Registry 가이드](schema-registry-guide.md)를 참고하세요.
