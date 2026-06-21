# 운영 Runbook

## 로컬 시작

```bash
make build
make up
make produce
make smoke
```

## 상태 확인

```bash
docker compose ps
curl http://localhost:8081/jobs
curl http://localhost:8000/health
make topics
make lag
```

기대 상태:

- Kafka가 healthy 상태입니다.
- Flink에 `flink-kraft-realtime-lab` job이 `RUNNING` 상태로 존재합니다.
- Generator 실행 후 `alerts.fraud`, `transactions.aggregates`, `transactions.dlq`에 메시지가 들어옵니다.

## 토픽 확인

```bash
make consume-alerts
make consume-aggregates
make consume-dlq
make replay-dlq
make consume-replay
```

## 자주 만나는 문제

### Flink job 출력이 없음

- Flink job이 시작된 뒤 `make produce`를 실행했는지 확인합니다.
- Aggregate는 event-time window가 닫힌 뒤 발행됩니다.
- Source offset은 `latest`에서 시작하므로, job 시작 전의 오래된 메시지는 의도적으로 건너뜁니다.

### Consumer lag 증가

- `make lag`를 실행합니다.
- Flink parallelism을 늘리기 전에 Kafka partition 수를 먼저 확인합니다.
- Flink UI에서 backpressure와 checkpoint failure를 확인합니다.

### DLQ record 증가

- Producer schema 변경 여부를 확인합니다.
- `reason`과 `errorType`을 확인합니다.
- `make replay-dlq`는 복구 가능한 record에만 사용합니다.

### Kubernetes custom resource 생성 실패

- Strimzi와 Flink Kubernetes Operator CRD가 설치되어 있는지 확인합니다.
- 먼저 `kubectl kustomize`로 manifest를 render합니다.
- Application pod보다 operator log를 먼저 확인합니다.

## 운영 환경과의 차이

- Kafka broker/controller를 3개 이상 사용합니다.
- Flink checkpoint storage는 durable storage를 사용합니다.
- TLS/authentication과 network policy를 추가합니다.
- Avro 또는 Protobuf 기반 Schema Registry를 추가합니다.
- Replay 권한과 audit trail을 정의합니다.
- Kafka lag, Flink checkpoint failure, backpressure, restart count, end-to-end latency를 모니터링합니다.
