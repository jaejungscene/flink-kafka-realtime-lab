# 실행 방법

이 문서는 프로젝트를 실제로 실행하는 방법을 Docker Compose와 Kubernetes 기준으로 나누어 설명합니다.

## 1. Docker Compose로 실행하기

Docker Compose는 가장 빠르게 프로젝트를 체험하는 방법입니다.

### 저장소 받기

```bash
git clone https://github.com/jaejungscene/flink-kafka-realtime-lab.git
cd flink-kafka-realtime-lab
```

### 사전 조건

- Docker Desktop 또는 OrbStack 실행 중
- `make`
- 사용 포트가 비어 있어야 합니다.

| 포트 | 서비스 |
| ---: | --- |
| `8000` | FastAPI |
| `8080` | Kafka UI |
| `8081` | Flink UI |
| `8085` | Schema Registry |
| `9090` | Prometheus |
| `3000` | Grafana |
| `5432` | PostgreSQL CDC source |
| `8083` | Kafka Connect |
| `29092` | Kafka host listener |

### 1단계: 빌드

```bash
make build
```

기대 결과:

- Flink job jar가 Docker image 안에서 Maven으로 빌드됩니다.
- API, generator, replayer image가 빌드됩니다.

### 2단계: 핵심 서비스 시작

```bash
make up
```

기대 결과:

- Kafka KRaft가 시작됩니다.
- `topic-init`이 topic을 생성합니다.
- Flink JobManager/TaskManager가 시작됩니다.
- `flink-submit`이 Flink job을 제출합니다.
- Kafka UI와 FastAPI가 시작됩니다.

확인:

```bash
docker compose ps
curl http://localhost:8081/jobs
curl http://localhost:8000/health
make topics
```

Flink job 상태가 `RUNNING`이면 정상입니다.

### 3단계: 이벤트 생성

```bash
make produce
```

기본 generator는 `transactions.raw`에 synthetic transaction event를 발행합니다.

짧게 실행하고 싶으면:

```bash
docker compose run --rm -e RUN_SECONDS=20 -e EVENTS_PER_SECOND=30 generator
```

### 4단계: 결과 확인

```bash
make smoke
```

또는 topic별로 직접 확인합니다.

```bash
make consume-alerts
make consume-aggregates
make consume-dlq
make lag
```

API로도 확인할 수 있습니다.

```bash
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=5"
curl "http://localhost:8000/topics/transactions.aggregates/messages?limit=5"
curl "http://localhost:8000/topics/transactions.dlq/messages?limit=5"
```

### 5단계: DLQ Replay

```bash
make replay-dlq
make consume-replay
```

`replayer`는 `transactions.dlq`에서 보정 가능한 record를 읽어 `transactions.replay`로 보냅니다. Flink job은 raw topic과 replay topic을 모두 소비합니다.

### 6단계: 종료

```bash
make down
```

`make down`은 volume까지 제거합니다. topic과 message도 함께 사라지므로, 실험을 처음부터 다시 시작할 때 사용합니다.

## 2. Docker Compose 실행 순서 요약

```bash
cd flink-kafka-realtime-lab
make build
make up
make produce
make smoke
make replay-dlq
make down
```

## 3. 선택 확장 실행

### Schema Registry

```bash
make schema-up
make schema-register
curl http://localhost:8085/subjects
```

자세한 설명은 [Schema Registry 가이드](schema-registry-guide.md)를 참고합니다.

### Prometheus/Grafana 관측성

```bash
make observe-up
make produce
curl http://localhost:8000/metrics
```

Grafana는 http://localhost:3000 에서 `admin/admin`으로 접속합니다. 자세한 설명은 [관측성 가이드](observability-guide.md)를 참고합니다.

### PostgreSQL CDC

```bash
make cdc-up
make cdc-register
make cdc-update-merchant
make consume-merchant-profiles
```

자세한 설명은 [CDC 가이드](cdc-guide.md)를 참고합니다.

### 장애/복구/부하 실습

```bash
make chaos-kill-taskmanager
make chaos-restart-kafka
make produce-high-load
make savepoint
```

자세한 설명은 [장애와 복구 실습 가이드](failure-recovery-guide.md)를 참고합니다.

## 4. Kubernetes로 실행하기

Kubernetes manifests는 Strimzi Kafka Operator와 Flink Kubernetes Operator를 사용하는 실무형 참고 구성입니다.

### 사전 조건

- Kubernetes cluster
- `kubectl`
- Strimzi Kafka Operator 설치
- Flink Kubernetes Operator 설치
- cluster에서 접근 가능한 container registry
- 아래 image를 registry에 push
  - `realtime-lab-flink-job:2.1.2`
  - `realtime-lab-api:latest`
  - `realtime-lab-generator:latest`

로컬 image 이름을 K8s manifest의 image 이름에 맞추는 예시는 아래와 같습니다.

```bash
docker build -t realtime-lab-flink-job:2.1.2 ./flink-job
docker build -t realtime-lab-api:latest ./api
docker build -t realtime-lab-generator:latest ./generator
```

원격 cluster라면 registry 경로를 붙여 push하고, `k8s/base/*.yaml` 또는 overlay에서 image 값을 registry 경로로 바꾸어야 합니다.

### 1단계: Manifest 렌더링

먼저 manifest가 정상적으로 렌더링되는지 확인합니다.

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/prod-like
```

파일로 저장해 검토하려면:

```bash
kubectl kustomize k8s/overlays/dev > /tmp/realtime-lab-dev.yaml
kubectl kustomize k8s/overlays/prod-like > /tmp/realtime-lab-prod-like.yaml
```

### 2단계: Overlay 선택

| Overlay | 사용 목적 | 특징 |
| --- | --- | --- |
| `k8s/overlays/dev` | 개발/학습 cluster | Kafka node 1개, ephemeral storage, stateless Flink upgrade |
| `k8s/overlays/prod-like` | 운영 유사 참고 | Kafka node 3개, replicated topic, persistent Kafka storage, savepoint upgrade |

처음에는 `dev` overlay를 권장합니다.

### 3단계: Dev Overlay 적용

```bash
kubectl apply -k k8s/overlays/dev
```

상태 확인:

```bash
kubectl -n realtime-lab get kafka,kafkanodepool,kafkatopic
kubectl -n realtime-lab get flinkdeployment
kubectl -n realtime-lab get pods
```

### 4단계: Kubernetes에서 Generator 실행

`k8s/base/generator-job.yaml`은 generator를 Kubernetes Job으로 실행합니다. 이미 overlay에 포함되어 있으므로 apply 시 함께 생성됩니다.

다시 실행하려면 기존 Job을 지운 뒤 재적용합니다.

```bash
kubectl -n realtime-lab delete job realtime-lab-generator --ignore-not-found
kubectl apply -k k8s/overlays/dev
```

### 5단계: API 접근

로컬에서 API를 확인하려면 port-forward를 사용합니다.

```bash
kubectl -n realtime-lab port-forward svc/realtime-lab-api 8000:8000
curl http://localhost:8000/health
curl "http://localhost:8000/topics/alerts.fraud/messages?limit=5"
```

### 6단계: 정리

```bash
kubectl delete -k k8s/overlays/dev
```

prod-like overlay를 적용했다면:

```bash
kubectl delete -k k8s/overlays/prod-like
```

## 5. Kubernetes 주의사항

- Strimzi와 Flink Operator CRD가 없으면 `Kafka`, `KafkaTopic`, `KafkaNodePool`, `FlinkDeployment` 리소스가 생성되지 않습니다.
- base image 이름은 예시입니다. 실제 cluster에서는 registry 경로를 붙여야 합니다.
- prod-like overlay도 실제 production 완성본은 아닙니다. TLS, auth, network policy, durable checkpoint storage, metrics, alerting을 추가해야 합니다.
- Flink checkpoint path는 production storage로 바꾸어야 합니다.

## 6. 문제 해결

| 증상 | 확인할 것 |
| --- | --- |
| Flink job이 안 뜸 | `docker compose logs flink-submit flink-jobmanager` 또는 `kubectl describe flinkdeployment` |
| topic이 없음 | `make topics`, `docker compose logs topic-init`, Strimzi `KafkaTopic` 상태 |
| aggregate가 바로 안 나옴 | 1분 event-time window가 닫혀야 출력됨 |
| DLQ가 많음 | `transactions.dlq`의 `errorType`, `reason`, producer schema 확인 |
| consumer lag 증가 | `make lag`, Flink UI backpressure/checkpoint 확인 |
