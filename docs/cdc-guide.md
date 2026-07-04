# CDC 가이드

CDC 예제는 PostgreSQL의 `merchant_risk_profiles` 변경을 Kafka topic으로 흘려보내고, Flink가 이를 Broadcast State로 유지해 transaction stream과 join하는 실행 경로입니다.

## 왜 추가했나

실제 기업에서는 모든 판단 기준이 event 안에 들어 있지 않습니다. 가맹점 위험 등급, 사용자 상태, 정책 threshold 같은 reference data는 DB에 있고, streaming job은 이를 함께 사용해야 합니다.

## 실행

```bash
make up
make cdc-up
make cdc-register
make consume-merchant-profiles
```

DB 값을 바꿔 CDC event를 발생시킵니다.

```bash
make cdc-update-merchant
```

알람에서 반영 여부를 확인합니다.

```bash
make produce
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=20"
```

## 구성

| 구성 요소 | 역할 |
| --- | --- |
| PostgreSQL | `merchant_risk_profiles` reference table |
| Debezium Kafka Connect | PostgreSQL 변경을 Kafka로 발행 |
| `merchant_risk_profiles` topic | compacted reference topic |
| Flink Broadcast State | profile을 모든 parallel task에 배포해 transaction과 join |

## Flink 반영 방식

Flink job은 `merchant_risk_profiles` topic을 earliest부터 읽습니다. 이 topic은 compacted topic이므로, job 재시작 시 최신 profile 상태를 다시 구성할 수 있습니다.

처리 규칙:

- profile이 없으면 multiplier `1.0`으로 처리합니다.
- profile이 있으면 `risk_multiplier`를 fraud score에 곱해 effective fraud score를 만듭니다.
- `manual_review_required=true`인 가맹점은 경계선 이벤트도 알람으로 승격될 수 있습니다.
- 깨진 profile event는 `REFERENCE_DATA_PARSE_ERROR`로 DLQ에 보냅니다.

실무에서는 profile 변경 시점과 이미 처리된 window를 다시 계산할지 여부를 별도 정책으로 정해야 합니다. 이 랩은 “변경 이후 들어오는 이벤트부터 새 profile을 적용”하는 단순하고 흔한 운영 모델을 사용합니다.
