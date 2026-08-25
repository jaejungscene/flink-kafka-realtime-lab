# 관측성 가이드

이 프로젝트는 Prometheus와 Grafana를 선택 profile로 제공합니다. 목표는 멋진 dashboard보다, 스트리밍 운영에서 어떤 신호를 봐야 하는지 익히는 것입니다.

## 실행

```bash
make up
make observe-up
make produce
make load-snapshot
```

접속:

| 도구 | URL |
| --- | --- |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

로컬 기본 계정은 `admin/admin`입니다. 공유 환경에서는 `.env`의
`GRAFANA_ADMIN_PASSWORD`를 반드시 바꾸십시오.

## 제공 metric

FastAPI의 `/metrics` endpoint가 Kafka offset을 읽어 Prometheus 형식으로 노출합니다.
Kafka 조회 부하를 줄이기 위해 결과를 기본 10초 동안 cache합니다. Flink runtime image에는
Prometheus reporter plugin이 포함되어 JobManager와 TaskManager의 `9249` port도 수집합니다.

| Metric | 의미 |
| --- | --- |
| `realtime_lab_up` | API process가 metric 응답을 만들 수 있는지 여부 |
| `realtime_lab_kafka_up` | Kafka metadata/offset 수집 성공 여부 |
| `realtime_lab_kafka_topic_available` | topic 존재 여부 |
| `realtime_lab_kafka_topic_retained_records` | partition의 `high-low` offset 차이 |
| `realtime_lab_kafka_topic_log_end_offset` | partition log end offset |
| `realtime_lab_kafka_consumer_lag` | `flink-realtime-lab` group의 partition별 lag |
| `realtime_lab_metrics_partition_errors` | offset 조회에 실패한 partition 수 |
| `realtime_lab_metrics_group_offset_errors` | committed offset 조회에 실패한 topic 수 |
| `realtime_lab_metrics_collection_duration_seconds` | 최근 Kafka metric 수집 소요 시간 |
| `realtime_lab_metrics_collection_timestamp_seconds` | 현재 cache snapshot 생성 시각 |
| `realtime_lab_metrics_last_success_timestamp_seconds` | 마지막 Kafka metadata 수집 성공 시각 |

Flink metric은 job, task, operator scope가 이름 앞에 붙습니다. Prometheus에서 다음 suffix로
검색하면 checkpoint, 처리량, 중복 제거 상태를 빠르게 찾을 수 있습니다.

| 검색 suffix | 의미 |
| --- | --- |
| `numRecordsIn`, `numRecordsOut` | operator 입력/출력 처리량 |
| `currentInputWatermark` | event-time 진행 상태 |
| `numberOfCompletedCheckpoints`, `numberOfFailedCheckpoints` | checkpoint 성공/실패 |
| `lastCheckpointDuration` | 최근 checkpoint 소요 시간 |
| `duplicate_events_total` | TTL state가 제거한 중복 event 수 |
| `merchant_profile_hits_total`, `merchant_profile_misses_total` | CDC profile enrichment 적용/미적용 수 |
| `merchant_profile_upserts_total`, `merchant_profile_deletes_total` | Broadcast State 변경 수 |

`retained_records`는 정확한 메시지 개수가 아닙니다. 삭제·압축으로 offset 사이에 빈
구간이 생길 수 있으므로 추세와 대략적인 backlog 크기를 보는 용도로만 사용합니다.

## 운영에서 봐야 할 질문

- `transactions.dlq`가 갑자기 늘었나?
- `alerts.fraud`가 평소보다 급증했나?
- `realtime_lab_kafka_consumer_lag`가 계속 증가하나?
- generator 부하를 올렸을 때 lag가 회복되는가?
- Flink UI에서 checkpoint 실패나 backpressure가 같이 보이는가?
- 배포 직후 merchant profile miss 비율이 정상 범위로 내려오는가?

부하 실험을 함께 보려면:

```bash
make load-experiment-small
```

자세한 해석 기준은 [부하/백프레셔 실험 가이드](load-backpressure-guide.md)를 참고합니다.

Prometheus rule은 API scrape 실패, Kafka metric 수집 실패·지연·stale, 구성된 topic
누락, consumer lag, partition offset 수집 오류를 감지합니다. topic별 합계는 별도
`_total` metric을 만들지 않고 PromQL의 `sum by (topic)`으로 계산합니다. 예제 threshold는
lab 부하에 맞춘 값이므로 실제 SLO로 그대로 사용하지 않습니다.

## 실무 확장

운영에서는 이 starter dashboard에 아래 지표를 추가하는 것이 좋습니다.

- Flink restart count
- Kafka broker network/request metric
- end-to-end latency
- DLQ reason별 count
