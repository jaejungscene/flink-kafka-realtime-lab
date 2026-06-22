# CDC 가이드

CDC 예제는 PostgreSQL의 `merchant_risk_profiles` 변경을 Kafka topic으로 흘려보내는 선택 실행 경로입니다. 실무에서 transaction stream과 reference data를 join하는 패턴을 이해하기 위한 기초입니다.

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

## 구성

| 구성 요소 | 역할 |
| --- | --- |
| PostgreSQL | `merchant_risk_profiles` reference table |
| Debezium Kafka Connect | PostgreSQL 변경을 Kafka로 발행 |
| `merchant_risk_profiles` topic | Flink가 join할 수 있는 reference stream 후보 |

## 다음 단계

현재 본편 Flink job은 CDC topic을 직접 join하지 않습니다. 의도적으로 분리한 이유는 본편의 fraud stream을 단순하게 유지하면서, 실무 확장 과제로 reference data join을 명확히 보여주기 위해서입니다.

실무 확장 방향:

- `merchant_risk_profiles`를 broadcast state로 유지
- transaction stream과 merchant profile stream을 `merchantId` 기준으로 enrich
- risk multiplier로 threshold를 동적으로 조정
- profile 변경 시 어떤 시점부터 rule에 반영할지 정책화
