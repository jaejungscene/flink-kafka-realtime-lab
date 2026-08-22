# 의존성 업데이트 가이드

이 저장소는 재현 가능한 학습을 위해 application, connector, container image 버전을 고정합니다.
고정은 업데이트를 멈춘다는 뜻이 아니므로 Dependabot이 매주 월요일 변경 후보를 PR로 만듭니다.

## 검토 순서

1. release note와 breaking change를 확인합니다.
2. Flink core와 Kafka connector의 호환 범위를 먼저 확인합니다.
3. `make lint`, `make test`, `make ci-smoke`, `make ci-smoke-exactly-once`를 실행합니다.
4. Docker Compose 전체 profile과 모든 Kustomize 디렉터리를 렌더링합니다.
5. state serializer 또는 operator topology가 바뀌면 기존 savepoint 복원을 별도로 검증합니다.

## Merge 원칙

- 자동 merge하지 않습니다.
- Flink, Kafka, connector는 서로 독립적으로 올리지 않고 호환성 근거를 남깁니다.
- major update는 별도 PR로 분리합니다.
- base image 변경 시 non-root UID, plugin 경로, healthcheck를 다시 확인합니다.
- Python과 Java patch update도 E2E 통과 전에는 merge하지 않습니다.

Dependabot은 후보를 알려 주는 도구일 뿐 운영 호환성을 보장하지 않습니다. 버전 선택 근거는
[버전 결정 기록](version-decision.md)에 반영합니다.
