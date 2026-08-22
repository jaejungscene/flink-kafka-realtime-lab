# CI E2E Smoke Test

이 문서는 GitHub Actions에서 Docker Compose 기반 E2E smoke test가 무엇을 검증하는지 설명합니다.

## 목적

정적 검증만으로는 Kafka, Flink, API, generator가 실제로 함께 동작하는지 알 수 없습니다. E2E smoke test는 CI에서 로컬 랩과 같은 핵심 경로를 실행해 “진짜로 메시지가 흐르는지” 확인합니다.

검증 범위:

- Kafka KRaft 시작
- topic 생성
- Flink JobManager/TaskManager 시작
- Flink job 제출과 `RUNNING` 상태 확인
- generator로 `transactions.raw` 이벤트 발행
- API health와 Compose readiness 확인
- `alerts.fraud` 메시지 생성 확인
- `transactions.aggregates` 메시지 생성 확인
- `transactions.dlq` 메시지 생성 확인
- DLQ summary API 응답 확인
- DLQ replay preview API 응답 확인
- DLQ replay API 실행 후 `transactions.replay` 메시지 생성 확인

## 실행

로컬에서도 CI와 같은 검증을 실행할 수 있습니다.

```bash
make ci-smoke
```

Exactly-once 모드도 같은 흐름으로 검증할 수 있습니다. GitHub Actions에서는 두 모드를
matrix의 독립 job으로 모두 실행합니다.

```bash
make ci-smoke-exactly-once
```

기본값:

| 환경변수 | 기본값 | 의미 |
| --- | ---: | --- |
| `CI_GENERATOR_RUN_SECONDS` | `80` | CI generator 실행 시간 |
| `CI_GENERATOR_EVENTS_PER_SECOND` | `40` | 초당 생성 이벤트 수 |
| `CI_FLINK_WAIT_ATTEMPTS` | `60` | Flink job RUNNING 대기 횟수 |
| `SMOKE_TOPIC_ATTEMPTS` | `35` | topic output 확인 재시도 횟수 |
| `SMOKE_TOPIC_SLEEP_SECONDS` | `3` | topic output 확인 재시도 간격 |

예시:

```bash
CI_GENERATOR_RUN_SECONDS=60 CI_GENERATOR_EVENTS_PER_SECOND=50 make ci-smoke
```

E2E 이전 job에서는 Java `mvn verify`, API/Python unit test와 Ruff, Compose 전체 profile
렌더링, Prometheus rule 검사, JSON/shell/Markdown 구조 검사, 모든 Kustomize 디렉터리 렌더링을 먼저
수행합니다. 별도 CDC smoke job은 Debezium connector와 task가 실제로 `RUNNING`이 되는지
확인한 뒤 PostgreSQL 초기 snapshot 레코드를 `merchant_risk_profiles`에서 직접 소비해
JSON payload까지 검증합니다. E2E는 정적·단위 검증이 통과한 뒤에만 시작합니다.

## 실패 시 확인할 것

`scripts/ci-e2e-smoke.sh`는 실패하면 자동으로 `docker compose ps`와 주요 서비스 로그를 출력하고 compose 환경을 정리합니다.

자주 보는 원인:

- Flink job이 `RUNNING`이 되기 전에 실패
- generator가 충분한 이벤트를 만들지 못해 window aggregate가 아직 나오지 않음
- API가 Kafka topic을 읽지 못함
- DLQ가 생성되지 않을 만큼 bad event 수가 부족함

## 실무 적용 포인트

이 테스트는 PR 단위의 핵심 경로 회귀 검증입니다. 성능, 장시간 state 복구, 보안,
schema compatibility, connector upgrade, 배포 후 canary까지 보장하지 않습니다.
