# 이벤트 스키마

현재 프로젝트는 JSON을 사용합니다. 실무에서는 Avro 또는 Protobuf와 Schema Registry를 붙이는 구성을 권장합니다.

## 토픽 목록

| Topic | 목적 |
| --- | --- |
| `transactions.raw` | 원천 결제/ML 이벤트 |
| `transactions.replay` | DLQ 보정 후 재처리 이벤트 |
| `transactions.aggregates` | Flink 실시간 집계 결과 |
| `transactions.aggregates.sql` | Flink SQL 집계 예제 결과 |
| `alerts.fraud` | Flink 알람 판단 결과 |
| `transactions.dlq` | 파싱/검증/late event 격리 |
| `merchant_risk_profiles` | PostgreSQL CDC 기반 reference data |

## `transactions.raw`와 `transactions.replay`

```json
{
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
  "ipRisk": 35
}
```

필수 field:

- `eventId`
- `userId`
- `eventTime`
- `amount >= 0`

## `alerts.fraud`

`alertType` 예시:

- `HIGH_RISK_TRANSACTION`
- `USER_PAYMENT_BURST`
- `MERCHANT_ANOMALY`

```json
{
  "alertId": "26a0b4c6-4f02-44e8-88c1-271a203d2a65",
  "alertType": "HIGH_RISK_TRANSACTION",
  "severity": "CRITICAL",
  "key": "user-001",
  "reason": "single event exceeded fraud rule threshold",
  "windowStart": 1760000000000,
  "windowEnd": 1760000000000,
  "eventTime": 1760000000000,
  "metricValue": 0.98,
  "sampleEventId": "8d6296df-8fdf-49fe-87a2-cf9476f54f3d"
}
```

## `transactions.aggregates`

```json
{
  "aggregateType": "COUNTRY_CATEGORY_1M",
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
  "errorType": "PARSE_OR_VALIDATION_ERROR",
  "reason": "eventId is required",
  "sourceTopic": "transactions.raw,transactions.replay",
  "replayTopic": "transactions.replay",
  "rawValue": "{\"eventId\":\"\"}",
  "observedAt": 1760000000000
}
```

`errorType` 예시:

- `PARSE_OR_VALIDATION_ERROR`
- `LATE_EVENT`

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

이 topic은 Flink broadcast state join을 학습하기 위한 reference data 예제입니다.

## Avro schema contract

`schemas/` 디렉터리에는 topic별 Avro schema 예제가 있습니다. 등록 방법은 [Schema Registry 가이드](schema-registry-guide.md)를 참고하세요.
