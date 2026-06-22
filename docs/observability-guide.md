# 관측성 가이드

이 프로젝트는 Prometheus와 Grafana를 선택 profile로 제공합니다. 목표는 멋진 dashboard보다, 스트리밍 운영에서 어떤 신호를 봐야 하는지 익히는 것입니다.

## 실행

```bash
make up
make observe-up
make produce
```

접속:

| 도구 | URL |
| --- | --- |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

Grafana 기본 계정은 `admin/admin`입니다.

## 제공 metric

FastAPI의 `/metrics` endpoint가 Kafka topic offset을 읽어 Prometheus 형식으로 노출합니다.

| Metric | 의미 |
| --- | --- |
| `realtime_lab_up` | API scrape 가능 여부 |
| `realtime_lab_kafka_topic_available` | topic 존재 여부 |
| `realtime_lab_kafka_topic_messages` | partition별 대략적인 message 수 |
| `realtime_lab_kafka_topic_messages_total` | topic별 대략적인 message 수 |
| `realtime_lab_kafka_consumer_lag` | `flink-realtime-lab` group의 partition별 lag |

## 운영에서 봐야 할 질문

- `transactions.dlq`가 갑자기 늘었나?
- `alerts.fraud`가 평소보다 급증했나?
- `realtime_lab_kafka_consumer_lag`가 계속 증가하나?
- generator 부하를 올렸을 때 lag가 회복되는가?
- Flink UI에서 checkpoint 실패나 backpressure가 같이 보이는가?

## 실무 확장

운영에서는 이 starter dashboard에 아래 지표를 추가하는 것이 좋습니다.

- Flink checkpoint duration/failure
- Flink restart count
- records in/out per second
- Kafka broker network/request metric
- end-to-end latency
- DLQ reason별 count
