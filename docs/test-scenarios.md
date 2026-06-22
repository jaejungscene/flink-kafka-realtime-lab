# 테스트 시나리오

이 문서는 이 프로젝트로 실험할 수 있는 스트리밍 처리 시나리오를 설명합니다. 각 시나리오는 학습자가 직접 topic을 관찰하면서 Flink/Kafka의 핵심 개념을 이해하도록 설계되어 있습니다.

## 준비

Docker Compose 기준으로 먼저 서비스를 시작합니다.

```bash
make build
make up
docker compose run --rm -e RUN_SECONDS=20 -e EVENTS_PER_SECOND=30 generator
```

공통 확인 명령:

```bash
make topics
make lag
make smoke
```

## 시나리오 1: 단건 고위험 결제 알람

목적:

- ML fraud score, 금액, IP risk 기반 단건 알람을 확인합니다.

발생 조건:

- `mlFraudScore >= 0.92`
- 또는 `amount >= 1000` and `ipRisk >= 80`
- 또는 실패 결제이고 fraud score/IP risk가 높은 경우

확인:

```bash
make consume-alerts
```

또는:

```bash
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=10"
```

기대 결과:

- `alertType`이 `HIGH_RISK_TRANSACTION`인 메시지가 `alerts.fraud`에 생성됩니다.
- `metricValue`에는 fraud score가 들어갑니다.
- `sampleEventId`로 원천 이벤트와 연결할 수 있습니다.

학습 포인트:

- 단건 알람은 window가 닫히기 전에도 빠르게 나올 수 있습니다.
- 모든 알람이 window 기반일 필요는 없습니다.

## 시나리오 2: 사용자별 1분 burst 알람

목적:

- keyed stream과 tumbling event-time window를 이해합니다.

발생 조건:

- 같은 `userId`가 1분 window 안에서 5회 이상 결제
- 또는 같은 `userId`의 1분 결제 총액이 3000 이상

확인:

```bash
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=20"
```

기대 결과:

- `alertType`이 `USER_PAYMENT_BURST`인 메시지가 생성됩니다.
- `windowStart`, `windowEnd`가 1분 window 범위를 나타냅니다.

학습 포인트:

- window 알람은 event-time watermark가 window 종료 시점을 지나야 출력됩니다.
- generator는 `user-burst` 패턴을 일부러 만들어 burst 알람이 발생하도록 합니다.

## 시나리오 3: 가맹점 이상 징후 알람

목적:

- merchant key 기준의 실시간 이상 탐지를 확인합니다.

발생 조건:

- 가맹점별 1분 거래량이 threshold 이상
- 또는 가맹점별 1분 거래 총액이 threshold 이상
- 또는 1분 평균 fraud score가 높음

확인:

```bash
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=30"
```

기대 결과:

- `alertType`이 `MERCHANT_ANOMALY`인 메시지가 생성됩니다.
- generator는 `merchant-hot` 패턴을 만들어 이 시나리오를 더 쉽게 관찰할 수 있게 합니다.

학습 포인트:

- 같은 이벤트 stream에서도 key를 다르게 잡으면 다른 종류의 실시간 판단을 만들 수 있습니다.
- user key와 merchant key는 서로 다른 운영 질문에 답합니다.

## 시나리오 4: 국가/카테고리/가맹점 1분 집계

목적:

- 실시간 집계 stream을 확인합니다.

집계 key:

```text
country|category|merchantId
```

확인:

```bash
make consume-aggregates
```

또는:

```bash
curl "http://localhost:8000/topics/transactions.aggregates/messages?limit=10"
```

기대 결과:

- `aggregateType`은 `COUNTRY_CATEGORY_1M`입니다.
- `eventCount`, `totalAmount`, `avgAmount`, `avgFraudScore`가 포함됩니다.

학습 포인트:

- aggregate stream은 dashboard, Redis, ClickHouse, Pinot, Druid, Elasticsearch 같은 serving/analytics storage로 이어질 수 있습니다.
- 집계는 즉시 나오지 않고 window와 watermark의 영향을 받습니다.

## 시나리오 5: 파싱/검증 실패 DLQ

목적:

- stream에서 깨진 이벤트를 job failure로 만들지 않고 DLQ로 격리하는 패턴을 확인합니다.

발생 조건:

- generator가 일부 malformed JSON을 생성합니다.
- `eventId`, `userId`, `eventTime`, `amount` 검증에 실패하면 DLQ로 이동합니다.

확인:

```bash
make consume-dlq
```

또는:

```bash
curl "http://localhost:8000/topics/transactions.dlq/messages?limit=10"
```

기대 결과:

- `errorType`이 `PARSE_OR_VALIDATION_ERROR`인 메시지가 생성됩니다.
- `reason`에 실패 원인이 들어갑니다.
- `rawValue`에 원본 메시지가 보존됩니다.

학습 포인트:

- Kafka source에서 바로 POJO 역직렬화를 하지 않고 string으로 받은 뒤 Flink 내부에서 parse하면 DLQ 처리가 쉬워집니다.
- DLQ는 단순 쓰레기통이 아니라 replay와 remediation의 출발점입니다.

## 시나리오 6: Late Event 처리

목적:

- event-time, watermark, allowed lateness의 의미를 확인합니다.

발생 조건:

- generator가 일부 이벤트의 `eventTime`을 과거로 밀어 넣습니다.
- Flink job은 10초 out-of-orderness와 30초 allowed lateness를 둡니다.
- 허용 범위를 넘은 이벤트는 late event로 DLQ에 기록됩니다.

확인:

```bash
curl "http://localhost:8000/topics/transactions.dlq/messages?limit=20"
```

기대 결과:

- `errorType`이 `LATE_EVENT`인 메시지가 보입니다.
- `replayTopic`은 `transactions.replay`입니다.

학습 포인트:

- late event를 무조건 버릴지, 별도 topic으로 보낼지, 보정 후 replay할지는 도메인 결정입니다.
- 정확성과 지연 시간은 tradeoff입니다.

## 시나리오 7: DLQ Replay

목적:

- DLQ 보정 후 재처리 흐름을 확인합니다.

실행:

```bash
make replay-dlq
make consume-replay
```

또는:

```bash
curl "http://localhost:8000/topics/transactions.replay/messages?limit=10"
```

기대 결과:

- 보정 가능한 DLQ record가 `transactions.replay`로 발행됩니다.
- Flink job은 raw topic과 replay topic을 모두 읽기 때문에 replay record도 다시 처리 대상이 됩니다.

학습 포인트:

- replay를 raw topic에 직접 넣지 않고 별도 topic에 넣으면 lineage와 운영 추적이 쉬워집니다.
- 실제 production에서는 replay 권한, audit log, ticket id, source DLQ offset을 함께 관리해야 합니다.

## 시나리오 8: Consumer Lag와 Checkpoint 확인

목적:

- 스트리밍 job이 정상적으로 Kafka를 따라가고 있는지 확인합니다.

확인:

```bash
make lag
curl http://localhost:8081/jobs
```

브라우저:

- Flink UI: http://localhost:8081
- Kafka UI: http://localhost:8080

학습 포인트:

- lag가 계속 증가하면 partition 수, Flink parallelism, backpressure, sink 지연을 함께 봐야 합니다.
- checkpoint 실패는 장애 복구와 exactly/at-least-once 처리 보장에 직접 연결됩니다.

## 시나리오 9: Kubernetes Manifest 검증

목적:

- Docker Compose와 Operator 기반 K8s 배포 모델의 차이를 확인합니다.

실행:

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/prod-like
```

확인할 차이:

- Kafka node 수
- Kafka storage type
- topic replication factor
- Flink TaskManager 수
- Flink upgrade mode
- checkpoint path placeholder

학습 포인트:

- 로컬 compose는 빠른 실험을 위한 환경입니다.
- K8s manifests는 팀 운영과 GitOps 구조를 이해하기 위한 참고 자료입니다.

## 시나리오 10: Schema Registry contract 확인

목적:

- Kafka topic별 schema contract를 어떻게 관리하는지 확인합니다.

실행:

```bash
make schema-up
make schema-register
curl http://localhost:8085/subjects
```

기대 결과:

- `transactions.raw-value`, `alerts.fraud-value`, `transactions.aggregates-value`, `transactions.dlq-value` subject가 등록됩니다.

학습 포인트:

- JSON만으로는 팀 간 event contract를 강제하기 어렵습니다.
- Schema Registry는 producer/consumer 변경을 review 가능한 계약으로 만듭니다.

## 시나리오 11: Observability와 lag 관찰

목적:

- Prometheus/Grafana로 DLQ, alert, topic message count, consumer lag를 관찰합니다.

실행:

```bash
make observe-up
make produce-high-load
make lag
```

확인:

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000
- API metrics: http://localhost:8000/metrics

학습 포인트:

- Lag는 단순 숫자가 아니라 처리량, partition 수, checkpoint, backpressure를 함께 봐야 하는 운영 신호입니다.

## 시나리오 12: 장애와 복구

목적:

- Flink TaskManager 장애와 Kafka 재시작 상황에서 job이 어떻게 회복되는지 확인합니다.

실행:

```bash
make chaos-kill-taskmanager
make chaos-restart-kafka
curl http://localhost:8081/jobs
make lag
```

학습 포인트:

- Checkpoint와 restart strategy는 장애 복구의 핵심입니다.
- 단일 Kafka broker는 학습용으로 충분하지만 운영 고가용성 구성은 아닙니다.

## 시나리오 13: CDC reference data

목적:

- DB 변경이 Kafka topic으로 전달되는 흐름을 확인합니다.

실행:

```bash
make cdc-up
make cdc-register
make cdc-update-merchant
make consume-merchant-profiles
```

학습 포인트:

- 실무 streaming rule은 event 자체뿐 아니라 DB의 reference data와 함께 판단되는 경우가 많습니다.
- 다음 확장 과제는 `merchant_risk_profiles` topic을 Flink broadcast state로 join하는 것입니다.

## 시나리오 14: Flink SQL 집계 비교

목적:

- 같은 1분 집계를 DataStream API와 Flink SQL로 어떻게 다르게 표현하는지 비교합니다.

확인:

```bash
sed -n '1,200p' flink-sql/country_category_merchant_aggregate.sql
```

학습 포인트:

- 반복적인 집계는 SQL이 간결합니다.
- DLQ, replay, 복잡한 rule test는 DataStream API가 더 명확할 수 있습니다.

## 추천 실험 순서

1. `make produce`로 raw event 생성
2. `alerts.fraud`에서 단건 알람 확인
3. 1분 뒤 `transactions.aggregates` 확인
4. `transactions.dlq`에서 parse failure와 late event 확인
5. `make replay-dlq`로 replay flow 확인
6. `make lag`와 Flink UI로 운영 지표 확인
7. `make observe-up`으로 Grafana dashboard 확인
8. `make schema-register`로 schema contract 확인
9. `make cdc-register`로 reference data CDC 확인
10. K8s overlay를 render해 compose와 운영형 배포 차이 비교

## 결과가 안 보일 때

- Aggregate/window 알람은 1분 window와 watermark가 지나야 나옵니다.
- 짧은 producer 실행 후 aggregate가 안 보이면 조금 기다리거나 generator를 더 오래 실행합니다.
- API는 기본적으로 최근 메시지 근처를 읽습니다. 처음부터 보고 싶으면 `from_beginning=true`를 붙입니다.

```bash
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=10&from_beginning=true"
```
