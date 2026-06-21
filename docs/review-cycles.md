# 5회 검토 Cycle 기록

이 프로젝트는 다섯 번의 "완성, 검토, 수정 보완, 테스트" cycle을 거쳐 구성했습니다.

## 1차 Cycle: 버전 기준 정리

완성:

- Kafka `4.1.2`와 Flink `2.1.2` 기준으로 프로젝트를 전환했습니다.
- Flink Kafka connector를 `4.0.1-2.0`으로 고정했습니다.

검토:

- Docker image 존재 여부
- Maven artifact 존재 여부
- Docker Compose 설정 정합성

수정 보완:

- `docs/version-decision.md`에 버전 선택 근거를 추가했습니다.

테스트:

- `docker compose config`
- Docker manifest check
- Maven metadata check

## 2차 Cycle: 스트리밍 핵심 시나리오

완성:

- Replay topic 소비를 추가했습니다.
- Merchant anomaly alert를 추가했습니다.
- Late event를 DLQ로 분리하는 처리를 추가했습니다.

검토:

- 각 시나리오가 실제 production streaming concern과 연결되는지 확인했습니다.

수정 보완:

- `transactions.replay` topic과 DLQ metadata field를 추가했습니다.
- Rule test를 보강했습니다.

테스트:

- High-risk, burst, merchant anomaly, replay eligibility rule test

## 3차 Cycle: Docker 운영성

완성:

- Lag, DLQ, replay, smoke check용 Makefile command를 추가했습니다.
- Replayer service를 추가했습니다.

검토:

- 처음 보는 학습자가 약 10분 안에 실행할 수 있는지 확인했습니다.

수정 보완:

- Smoke check가 alerts, aggregates, DLQ를 모두 확인하도록 확장했습니다.

테스트:

- Docker Compose render
- Python compile check

## 4차 Cycle: Kubernetes

완성:

- Strimzi Kafka와 Flink Kubernetes Operator manifests를 추가했습니다.
- `dev`, `prod-like` overlay를 추가했습니다.

검토:

- GitOps/operator workflow에 익숙한 실무자가 참고할 수 있는 구조인지 확인했습니다.

수정 보완:

- Local dev 설정과 prod-like replication/resource 설정을 분리했습니다.

테스트:

- `kubectl kustomize k8s/overlays/dev`
- `kubectl kustomize k8s/overlays/prod-like`

## 5차 Cycle: 학습과 인수인계 문서

완성:

- README, learning guide, schema docs, runbook, replay guide, Kubernetes guide, CI를 정리했습니다.

검토:

- 학습자에게 유익한지
- 기업 실무 참고 가치가 있는지
- 면접과 협업 상황에서 설명 가능한지

수정 보완:

- Local에서 바로 실행 가능한 부분과 production에서 반드시 바꿔야 하는 부분을 문서화했습니다.

테스트:

- Static check
- Manifest render check
- CI workflow coverage
