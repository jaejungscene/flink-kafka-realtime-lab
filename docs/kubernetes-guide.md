# Kubernetes 가이드

이 문서는 Docker Compose를 넘어 Kubernetes에서 프로젝트를 배포하고 싶은 사용자를 위한 안내입니다. 이 저장소의 Kubernetes 구성은 Strimzi Kafka Operator와 Flink Kubernetes Operator를 전제로 합니다.

## 사전 조건

- Kubernetes cluster
- KRaft와 KafkaNodePool을 지원하는 Strimzi Kafka Operator
- Flink Kubernetes Operator
- cluster에서 접근 가능한 container registry
- registry에 push된 container image
  - `realtime-lab-flink-job:2.1.2`
  - `realtime-lab-api:latest`
  - `realtime-lab-generator:latest`

## 매니페스트 렌더링

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/prod-like
```

## 적용 순서

```bash
kubectl apply -k k8s/overlays/dev
kubectl -n realtime-lab get kafka,kafkatopic,flinkdeployment,pod
```

`Kafka`, `KafkaTopic`, `KafkaNodePool`, `FlinkDeployment`는 custom resource입니다. 따라서 Strimzi와 Flink Operator CRD가 먼저 설치되어 있어야 합니다.

## 개발용 Overlay

`dev` overlay는 가볍게 학습하고 실험하기 위한 구성입니다.

- Kafka broker 1개
- dual-role KafkaNodePool node 1개
- ephemeral Kafka storage
- Flink TaskManager 1개
- stateless Flink upgrade mode
- 짧은 generator 실행

## 운영 유사 Overlay

`prod-like` overlay는 실제 운영에서 고려할 선택지를 보여주기 위한 참고 구성입니다.

- Kafka broker 3개
- dual-role KafkaNodePool node 3개
- persistent Kafka storage
- topic replication factor 3
- Flink TaskManager 2개
- savepoint upgrade mode
- checkpoint path placeholder

실제 production에 적용하기 전에는 placeholder image 이름을 registry 경로로 바꾸고, TLS/auth, durable checkpoint storage, metrics, alerting, network policy를 회사 환경에 맞게 추가해야 합니다.
