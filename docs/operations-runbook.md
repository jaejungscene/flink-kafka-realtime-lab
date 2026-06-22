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
curl http://localhost:8000/metrics
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

## 선택 확장 상태 확인

```bash
curl http://localhost:8085/subjects
curl http://localhost:9090/-/ready
curl http://localhost:3000/api/health
curl http://localhost:8083/connectors
```

각 명령은 Schema Registry, Prometheus, Grafana, Kafka Connect가 정상인지 확인합니다. 선택 profile을 켜지 않았다면 실패하는 것이 정상입니다.

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

### Schema Registry 등록 실패

- `make schema-up`을 먼저 실행했는지 확인합니다.
- `curl http://localhost:8085/subjects`가 응답하는지 확인합니다.
- Avro schema JSON이 깨졌는지 `schemas/*.avsc`를 확인합니다.

### Kafka Connect connector 등록 실패

- `make cdc-up` 이후 Connect REST API가 준비될 때까지 잠시 기다립니다.
- `curl http://localhost:8083/connectors`를 확인합니다.
- PostgreSQL container가 실행 중인지 `docker compose ps postgres`로 확인합니다.

### Grafana dashboard가 비어 있음

- `make observe-up` 뒤 `make produce`를 실행했는지 확인합니다.
- `curl http://localhost:8000/metrics`에 metric이 나오는지 확인합니다.
- Prometheus target `realtime-lab-api`가 up인지 확인합니다.

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
- CDC reference data join은 broadcast state, TTL, schema evolution 정책까지 함께 설계합니다.
