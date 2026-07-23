# 전체 품질 검토 기록

이 문서는 저장소를 10개 관점으로 나누어 검토하고 수정한 결과를 기록합니다.
단순히 기능 목록을 나열하지 않고, 발견한 위험과 실제 변경 사항, 남아 있는 한계를
구분합니다.

## 1. 프로젝트 구조와 코드 규칙

발견한 문제:

- 예제용 `com.example` package가 그대로 남아 소유권과 artifact 좌표가 불명확했습니다.
- CLI 인자가 main class에 흩어져 있고 잘못된 값이나 오타를 조용히 허용했습니다.
- Kafka sink 생성 코드가 반복되어 delivery guarantee 설정이 달라질 여지가 있었습니다.

수정:

- Java package와 Maven group을 `io.github.jaejungscene`으로 통일했습니다.
- `JobConfig`에서 지원 인자, 중복, 필수 형식, topic 충돌, 시간 범위를 검증합니다.
- `KafkaSinkFactory`로 직렬화와 transactional ID 생성을 한곳에서 관리합니다.

## 2. Kafka 전달 보장과 레코드 계보

발견한 문제:

- value-only 역직렬화 때문에 DLQ에 실제 source partition/offset을 기록할 수 없었습니다.
- alert ID가 매번 무작위라 같은 입력을 재처리하면 dedup 기준이 달라졌습니다.

수정:

- Kafka topic, partition, offset, timestamp, key, value를 `KafkaRecord`로 보존합니다.
- DLQ와 replay event에 원본 좌표를 전달합니다.
- alert ID를 rule/window/key/sample event 기반의 결정적 UUID로 생성합니다.

## 3. Event time과 late data

발견한 문제:

- 유휴 partition이 전체 watermark 진행을 막을 수 있었습니다.
- 미래 시각과 위험도 범위를 검증하지 않아 window가 비정상적으로 진행될 수 있었습니다.
- 서로 다른 단위인 금액과 score 중 큰 값을 metric으로 기록했습니다.

수정:

- watermark idleness와 허용 가능한 미래 시각을 설정으로 분리했습니다.
- `amount`, `eventTime`, `mlFraudScore`, `ipRisk` 범위를 검증합니다.
- alert에 `metricName`을 추가하고 `metricValue`의 단위를 일관되게 유지합니다.

## 4. CDC와 Broadcast State

발견한 문제:

- Debezium delete marker 설정과 parser 기대값이 달라 삭제가 state에 반영되지 않았습니다.
- 잘못된 risk tier와 숫자 값을 기본값으로 바꾸어 데이터 오류를 숨겼습니다.

수정:

- delete rewrite event를 사용하고 `__deleted`를 명시적으로 처리합니다.
- risk tier, multiplier, boolean field를 엄격히 검증합니다.
- 잘못된 reference event는 `REFERENCE_DATA_PARSE_ERROR` DLQ로 분리합니다.

## 5. DLQ와 replay 안전성

발견한 문제:

- late event, 음수 금액, 누락된 사용자처럼 의미를 추론할 수 없는 데이터까지 자동
  보정했습니다.
- API 실행이 preview 결과와 연결되지 않아 스캔 사이에 대상이 바뀔 수 있었습니다.
- CLI consumer offset이 Kafka publish 확인 전에 commit될 수 있었습니다.

수정:

- 자동 replay는 원본 JSON이 유효한 `PARSE_OR_VALIDATION_ERROR`로 제한합니다.
- `userId`, 원래 event time, 유효한 금액과 위험도는 보존하며 누락된 `eventId`만
  source offset 기반으로 결정적으로 생성할 수 있습니다.
- preview에서 고른 partition/offset, 동일한 `replay_run_id`, `confirm=true`가 있어야
  실행됩니다.
- producer delivery를 확인한 뒤에만 CLI consumer offset을 commit합니다.

## 6. 관측성과 장애 복구

발견한 문제:

- Prometheus scrape마다 모든 Kafka offset을 다시 조회했습니다.
- API process 생존과 Kafka 연결 준비 상태가 하나의 health endpoint에 섞여 있었습니다.
- offset 차이를 정확한 메시지 수처럼 표현했습니다.

수정:

- metric 결과를 짧게 cache하고 partition별 수집 오류를 격리합니다.
- `/health`와 `/ready`를 분리합니다.
- metric을 `retained_records` 추정치로 명명하고 Kafka 상태, lag, 수집 오류 alert rule을
  추가했습니다.
- TaskManager 장애 실습은 job이 다시 `RUNNING`이 될 때까지 확인합니다.

## 7. Docker Compose와 Kubernetes

발견한 문제:

- 고정된 `sleep` 뒤 job을 제출해 시작 순서에 민감했습니다.
- Compose checkpoint와 savepoint가 컨테이너 수명에 묶였습니다.
- K8s API에 readiness, resource, container security 설정이 부족했습니다.

수정:

- JobManager 준비 상태를 확인한 뒤 job을 제출합니다.
- Compose named volume에 checkpoint/savepoint를 보존합니다.
- K8s liveness/readiness, resource request/limit, non-root, read-only root filesystem,
  capability drop을 적용했습니다.
- `prod-like` API에 2 replicas, topology spread, PDB를 추가했습니다.

`prod-like` overlay의 object storage 경로와 image는 의도적인 placeholder입니다. 실제
plugin, 인증, TLS, NetworkPolicy가 없으므로 production manifest로 간주하지 않습니다.

## 8. 테스트와 CI

수정:

- Maven/Java 실행 버전을 enforcer로 고정하고 `mvn verify`를 품질 게이트로 사용합니다.
- Kafka envelope, parser, CDC delete, deterministic alert ID, sink ID, strict JSON parser를
  단위 테스트합니다.
- FastAPI replay guard, token, topic allowlist, metric cache를 Docker test stage에서
  검증합니다.
- CI에서 Compose profile, Prometheus rule, JSON, shell, Kustomize overlay를 검증하고
  마지막에 Compose E2E smoke test를 실행합니다.

## 9. 보안과 설정 관리

발견한 문제:

- API에서 Kafka Connect 내부 topic까지 임의로 조회할 수 있었습니다.
- replay endpoint에 접근 제어 경계가 없었습니다.
- PostgreSQL과 Grafana 비밀번호가 구성에 고정되어 있었습니다.

수정:

- `READABLE_TOPICS` allowlist와 선택적 `API_TOKEN` 인증을 추가했습니다.
- K8s token은 optional Secret에서 읽고, 로컬 비밀번호는 `.env`로 덮어쓸 수 있습니다.
- Debezium connector 비밀번호는 Kafka Connect `EnvVarConfigProvider`로 주입합니다.
- producer는 idempotence와 delivery callback을 사용하고 실패를 성공으로 보고하지 않습니다.

## 10. 문서와 표현

수정:

- 오래된 repository/package 경로와 실제 코드에 맞지 않는 replay 설명을 고쳤습니다.
- Kafka offset 차이를 정확한 건수처럼 표현하던 문장을 추정치로 수정했습니다.
- `EXACTLY_ONCE`의 범위를 Kafka output commit으로 한정하고 외부 side effect 보장은
  별도임을 명시했습니다.
- 로컬 실행 구성, 검토용 `prod-like` overlay, 실제 production 요구사항을 구분했습니다.

## 검증 기준

변경 완료 후 다음을 함께 통과해야 합니다.

- Java unit test와 shaded JAR build
- Python helper/API contract test
- Python compile, shell syntax, JSON/Avro JSON 구문 검사
- Docker Compose 전체 profile 렌더링
- Prometheus config/rule 검사
- Kubernetes dev, exactly-once, prod-like overlay 렌더링
- Kafka → Flink → alert/aggregate/DLQ → replay Compose E2E smoke test

이 검증은 회귀를 줄이는 장치이지 성능, 보안, 고가용성, 데이터 정확성에 대한 production
인증은 아닙니다.
