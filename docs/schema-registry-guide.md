# Schema Registry 가이드

이 프로젝트의 기본 실행은 JSON입니다. Schema Registry 예제는 실무에서 자주 필요한 schema contract와 schema evolution을 학습하기 위한 선택 실행 경로입니다.

## 왜 추가했나

- Producer와 Consumer가 같은 event contract를 공유하도록 만들기 위해서입니다.
- field 추가, 삭제, 타입 변경이 downstream job을 깨뜨리는지 미리 검증할 수 있습니다.
- 여러 팀이 같은 Kafka topic을 사용할 때 구두 약속이 아니라 schema로 협업할 수 있습니다.

## 실행

```bash
make up
make schema-up
make schema-register
```

확인:

```bash
curl http://localhost:8085/subjects
curl http://localhost:8085/subjects/transactions.raw-value/versions/latest
```

## 포함된 스키마

| 파일 | Subject | 목적 |
| --- | --- | --- |
| `schemas/transactions-raw-value.avsc` | `transactions.raw-value`, `transactions.replay-value` | 원천/replay 결제 이벤트 |
| `schemas/alerts-fraud-value.avsc` | `alerts.fraud-value` | fraud 알람 결과 |
| `schemas/transactions-aggregates-value.avsc` | `transactions.aggregates-value` | window 집계 결과 |
| `schemas/transactions-dlq-value.avsc` | `transactions.dlq-value` | DLQ 격리 이벤트 |

## 실무 규칙

- 새 field는 기본값이 있는 optional field로 추가합니다.
- 기존 field의 의미를 바꾸지 않습니다.
- required field 삭제나 타입 변경은 breaking change로 봅니다.
- DLQ field는 remediation과 audit에 필요하므로 너무 쉽게 줄이지 않습니다.

## 현재 범위

현재 Flink job은 JSON을 계속 읽고 씁니다. Avro serialization까지 연결하지 않은 이유는 학습자가 먼저 Kafka/Flink 흐름을 이해한 뒤 schema governance를 별도 주제로 실험할 수 있게 하기 위해서입니다.
