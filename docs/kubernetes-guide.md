# Kubernetes 가이드

이 문서는 Docker Compose를 넘어 Kubernetes에서 프로젝트를 배포하고 싶은 사용자를 위한 안내입니다. 이 저장소의 Kubernetes 구성은 Strimzi Kafka Operator와 Flink Kubernetes Operator를 전제로 합니다.

## 사전 조건

- Kubernetes cluster
- KRaft와 KafkaNodePool을 지원하는 Strimzi Kafka Operator
- Flink Kubernetes Operator
- cluster에서 접근 가능한 container registry
- registry에 push된 container image
  - `realtime-lab-flink-job:2.1.2`
  - `realtime-lab-api:1.0.0`
  - `realtime-lab-generator:1.0.0`

## 매니페스트 렌더링

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/prod-like
kubectl kustomize k8s/overlays/exactly-once
```

## 적용 순서

```bash
kubectl apply -k k8s/overlays/dev
kubectl -n realtime-lab get kafka,kafkatopic,flinkdeployment,pod
```

Flink job이 `RUNNING`인 것을 확인한 뒤 테스트 event를 별도로 생성합니다.

```bash
make k8s-run-generator
kubectl -n realtime-lab logs job/realtime-lab-generator -f
```

Generator를 core overlay와 분리한 이유는 `LATEST` source가 준비되기 전에 Job이 먼저
종료되어 초기 event를 놓치는 배포 race를 막기 위해서입니다.

`Kafka`, `KafkaTopic`, `KafkaNodePool`, `FlinkDeployment`는 custom resource입니다. 따라서 Strimzi와 Flink Operator CRD가 먼저 설치되어 있어야 합니다.

API는 모든 overlay에서 Secret을 필수로 요구합니다. dev와 exactly-once overlay는 학습용
`dev-only-change-me` token을 생성하므로 공유 환경에 그대로 노출하면 안 됩니다. prod-like는
Secret을 저장소에서 만들지 않으므로 apply 전에 secret manager 또는 다음 예시로 생성합니다.

```bash
kubectl apply -f k8s/base/namespace.yaml
kubectl -n realtime-lab create secret generic realtime-lab-api-secrets \
  --from-literal=api-token='replace-with-a-strong-token'
```

## 개발용 Overlay

`dev` overlay는 가볍게 학습하고 실험하기 위한 구성입니다.

- Kafka broker 1개
- dual-role KafkaNodePool node 1개
- ephemeral Kafka storage
- Flink TaskManager 1개
- stateless Flink upgrade mode
- opt-in generator Job은 Flink 준비 확인 후 별도 실행
- API 개발용 token은 `dev-only-change-me`
- Flink JobManager/TaskManager는 UID/GID `9999` non-root로 실행

## Prod-like 검토 Overlay

`prod-like` overlay는 개발 구성과 다중 노드 구성의 차이를 렌더링해 검토하기 위한
예시입니다.

- Kafka broker 3개
- dual-role KafkaNodePool node 3개
- persistent Kafka storage
- topic replication factor 3
- Flink TaskManager 2개
- Kubernetes HA와 standby JobManager 2개
- savepoint upgrade mode
- checkpoint/savepoint object storage placeholder
- API 2 replicas, topology spread, PodDisruptionBudget

placeholder image와 `s3://replace-me-realtime-lab/...` 경로를 바꾸는 것만으로는 충분하지
않습니다. Flink image에는 S3 filesystem plugin이 포함되어 있지만 cluster의 workload identity
또는 secret 기반 인증을 연결해야 합니다. 또한 Kafka TLS/auth,
NetworkPolicy, secret manager, Prometheus discovery와 alert routing을 환경에 맞게 설계해야
합니다. 준비 전에는 `kubectl apply`하지 말고
`k8s/overlays/prod-like/README.md`의 점검 목록을 따르십시오.
