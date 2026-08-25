# 부하/백프레셔 실험 가이드

이 문서는 Kafka/Flink 스트리밍 파이프라인에 부하를 주고, lag와 backpressure 신호를 확인하는 방법을 설명합니다.

## 목적

실무에서는 “정상 처리된다”보다 “부하가 늘 때 어디가 먼저 밀리는지”를 설명할 수 있어야 합니다.

이 실험에서 보는 신호:

- Kafka consumer lag
- topic별 message 증가량
- Flink job/vertex 상태
- Flink backpressure endpoint 응답
- DLQ 증가 여부

## 빠른 실행

```bash
make build
make up
make observe-up
make load-experiment-small
```

조금 더 강하게 실행하려면:

```bash
make load-experiment
```

부하 크기를 직접 조정하려면:

```bash
LOAD_RUN_SECONDS=300 \
LOAD_EVENTS_PER_SECOND=250 \
LOAD_SNAPSHOT_INTERVAL_SECONDS=30 \
make load-experiment
```

같은 데이터 분포와 event ID로 비교하려면 seed를 고정합니다.

```bash
GENERATOR_RANDOM_SEED=42 make produce-high-load
```

## Make Target

| Target | 의미 |
| --- | --- |
| `make produce-high-load` | generator만 높은 EPS로 실행 |
| `make load-snapshot` | 현재 Flink job, vertex, lag, 보존 레코드 추정치를 한 번 출력 |
| `make load-experiment-small` | 60초 동안 중간 부하를 주고 20초마다 snapshot 출력 |
| `make load-experiment` | 180초 동안 높은 부하를 주고 30초마다 snapshot 출력 |

## 관찰 순서

1. `make load-snapshot`으로 기준 상태를 봅니다.
2. `make load-experiment-small`로 가벼운 부하를 줍니다.
3. lag가 증가했다가 줄어드는지 봅니다.
4. Flink UI에서 busy/backpressure/checkpoint를 확인합니다.
5. `transactions.dlq`가 같이 증가하면 데이터 품질 문제인지 처리 지연 문제인지 분리합니다.

## 해석 기준

| 현상 | 해석 |
| --- | --- |
| lag가 잠깐 올랐다가 내려감 | 일시 부하를 따라잡는 상태 |
| lag가 계속 증가 | 처리량이 입력량보다 낮음 |
| 특정 vertex만 backpressure 높음 | 해당 operator 또는 downstream sink 병목 가능성 |
| checkpoint duration 증가 | state, sink commit, 리소스 압박 확인 필요 |
| DLQ 증가 | schema/validation/late event 원인 확인 필요 |

## 실무 튜닝 포인트

- Generator는 client queue가 일시적으로 가득 차면 최대 10초 동안 `poll`하며 재시도합니다.
  이 시간이 지나도 queue가 비워지지 않으면 성공으로 숨기지 않고 실행을 실패시킵니다.
- 목표 EPS는 produce 시간에 drift되지 않도록 monotonic deadline으로 조절합니다.
- Kafka partition 수와 Flink parallelism을 함께 봅니다.
- source 처리량만 보지 말고 sink commit, checkpoint, window operator를 같이 봅니다.
- `EXACTLY_ONCE` 모드는 transaction/checkpoint 비용 때문에 같은 부하에서 더 늦게 보일 수 있습니다.
- 로컬 Docker 단일 broker 결과를 실제 용량 계획에 일반화하지 않습니다.
- 운영에서는 Flink metric reporter, Kafka broker metric, end-to-end latency metric을 함께 둡니다.

## 정리

```bash
make down
```

`make down`은 volume까지 지우므로 topic data도 사라집니다.
