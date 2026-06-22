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

## 확장 1차 Cycle: Schema Registry

완성:

- `schemas/`에 topic별 Avro schema 예제를 추가했습니다.
- Schema Registry와 schema 등록 script를 Docker Compose profile로 추가했습니다.

검토:

- 기본 JSON 실행 경로를 깨지 않고 schema governance를 학습할 수 있는지 확인했습니다.

수정 보완:

- 본편 serialization 변경 대신 선택 실행으로 분리해 학습 난도를 낮췄습니다.

테스트:

- `docker compose config`
- Avro schema JSON parse

## 확장 2차 Cycle: 관측성

완성:

- FastAPI `/metrics` endpoint를 추가했습니다.
- Prometheus scrape config와 Grafana starter dashboard를 추가했습니다.

검토:

- 실무자가 먼저 보는 lag, DLQ, alert, topic count가 드러나는지 확인했습니다.

수정 보완:

- Flink 내부 metric 전체를 억지로 붙이지 않고, lab에서 안정적으로 볼 수 있는 Kafka/API metric부터 제공했습니다.

테스트:

- Python compile
- Prometheus/Grafana provisioning 파일 parse

## 확장 3차 Cycle: 장애와 복구

완성:

- TaskManager kill/restart, Kafka restart, high-load produce, savepoint target을 추가했습니다.

검토:

- 장애 실습이 destructive하지 않고 로컬 compose 안에서 회복 가능한지 확인했습니다.

수정 보완:

- `make` target을 짧게 만들고 상세 설명은 `docs/failure-recovery-guide.md`로 분리했습니다.

테스트:

- Shell script syntax check
- `docker compose config`

## 확장 4차 Cycle: CDC와 reference data

완성:

- PostgreSQL, Debezium Kafka Connect, connector 등록 script, merchant risk profile seed data를 추가했습니다.

검토:

- Fraud stream 본편을 복잡하게 만들지 않으면서 실무 reference data join 주제를 보여주는지 확인했습니다.

수정 보완:

- CDC topic을 본편 join에 바로 연결하지 않고 확장 과제로 문서화했습니다.

테스트:

- Connector JSON parse
- Docker Compose profile render

## 확장 5차 Cycle: Flink SQL과 문서 연결

완성:

- 동일 집계 요구사항을 표현한 Flink SQL 예제를 추가했습니다.
- README, 실행 문서, 시나리오 문서, 운영 runbook을 확장 기능과 연결했습니다.

검토:

- 학습자와 실무자가 어떤 순서로 보면 좋은지 문서에서 바로 보이는지 확인했습니다.

수정 보완:

- 각 확장 주제를 별도 가이드로 나누어 README가 과하게 길어지지 않게 했습니다.

테스트:

- Markdown fence check
- K8s overlay render
- 전체 정적 검증
