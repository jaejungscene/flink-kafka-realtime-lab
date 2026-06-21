# 학습 가이드

## 1. Kafka KRaft

Kafka 4.x는 KRaft-only 방향입니다. 이 프로젝트는 Docker Compose에서는 단일 broker/controller를 사용하고, Kubernetes에서는 Strimzi 기반 cluster 구성을 제공해서 local 개발 환경과 operator 기반 배포 환경을 비교할 수 있게 합니다.

핵심 아이디어:

- Topic은 명시적으로 생성합니다.
- Partition 수는 시스템 설계의 일부입니다.
- Consumer lag는 단순 Kafka 지표가 아니라 운영 신호입니다.
- Replay는 raw ingestion과 분리해 lineage를 보존합니다.

## 2. Flink Event Time

Flink는 container가 record를 받은 시간이 아니라 event 자체의 `eventTime`을 기준으로 처리합니다.

```java
WatermarkStrategy
    .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
```

이 job은 `allowedLateness(Duration.ofSeconds(30))`도 사용합니다. 허용 지연 시간을 넘어 도착한 event는 DLQ성 처리로 분리됩니다.

## 3. 알람 패턴

이 프로젝트에는 실무에서 자주 쓰는 세 가지 알람 패턴이 들어 있습니다.

- 단건 위험 알람: 이벤트 하나만으로도 위험한 거래를 탐지합니다.
- 사용자 burst 알람: 한 사용자가 1분 동안 너무 자주 또는 너무 큰 금액을 결제하는지 봅니다.
- 가맹점 이상 징후: 가맹점의 거래량, 거래 총액, 평균 fraud score가 비정상적으로 높은지 봅니다.

Rule은 `RiskRules`에 분리되어 있습니다. 그래서 전체 Flink topology를 읽지 않아도 threshold를 바꾸거나 테스트를 추가할 수 있습니다.

## 4. 집계 패턴

`transactions.aggregates`는 1분 단위 `country|category|merchant` 집계를 발행합니다.

- event count
- total amount
- average amount
- average fraud score

실무에서는 이 stream이 Redis, ClickHouse, Pinot, Druid, Elasticsearch, BigQuery 또는 real-time dashboard로 이어질 수 있습니다.

## 5. DLQ와 Replay

이 job은 Kafka value를 먼저 string으로 읽고, Flink 내부에서 JSON parse를 수행합니다. 이렇게 하면 bad event 처리가 명시적으로 드러납니다.

- malformed JSON
- required field 누락
- negative amount
- too-late event

복구 가능한 DLQ record는 replayer가 보정한 뒤 `transactions.replay`로 다시 발행할 수 있습니다.

## 6. 추천 실습

1. `RiskRules`의 fraud threshold를 바꾸고 `make test`를 실행합니다.
2. `EVENTS_PER_SECOND`를 높이고 `make lag`로 consumer lag를 관찰합니다.
3. Watermark delay를 바꾸고 aggregate latency를 비교합니다.
4. `make replay-dlq`를 실행하고 `transactions.replay`를 확인합니다.
5. Kubernetes overlay를 render해서 dev와 prod-like 운영 선택지를 비교합니다.
