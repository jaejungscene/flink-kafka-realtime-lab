# 전체 품질 검토 기록

이 문서는 저장소를 10개 관점으로 나누어 수행한 1차 검토와, 같은 기준을 다시 적용한
2차 전수 검토, 작은 경계와 표현까지 다시 확인한 3차 세부 검토 결과를 기록합니다.
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
- 단건 alert ID는 source event를 포함하고, window alert ID는 rule/window/key를 사용하는
  결정적 UUID로 생성합니다.

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
- K8s base는 선택적 Secret을 읽고 prod-like overlay는 token Secret을 필수로 요구합니다.
  로컬 비밀번호는 `.env`로 덮어쓸 수 있습니다.
- Debezium connector 비밀번호는 Kafka Connect `EnvVarConfigProvider`로 주입합니다.
- producer는 idempotence와 delivery callback을 사용하고 실패를 성공으로 보고하지 않습니다.

## 10. 문서와 표현

수정:

- 오래된 repository/package 경로와 실제 코드에 맞지 않는 replay 설명을 고쳤습니다.
- Kafka offset 차이를 정확한 건수처럼 표현하던 문장을 추정치로 수정했습니다.
- `EXACTLY_ONCE`의 범위를 Kafka output commit으로 한정하고 외부 side effect 보장은
  별도임을 명시했습니다.
- 로컬 실행 구성, 검토용 `prod-like` overlay, 실제 production 요구사항을 구분했습니다.

## 2차 전수 검토: 11–20

### 11. 빌드 경계와 저장소 위생

- root, Flink, generator build context에 `.dockerignore`를 추가해 `.git`, 문서, 로컬
  산출물이 이미지 빌드로 전달되지 않게 했습니다.
- Make target 선언과 test image 정리를 일관되게 정리했습니다.

### 12. JSON과 Avro 계약 일치

- Avro 예제에만 있던 `schemaVersion`을 transaction, alert, aggregate, DLQ 실제 JSON에도
  포함했습니다.
- event/user/merchant 식별자의 공백과 길이, 잘못된 schema version을 입력 경계에서
  검증합니다.

### 13. Window late cutoff와 결정적 결과

- event time에 allowed lateness만 더해 너무 일찍 버리던 로직을 window cleanup deadline
  기준으로 수정했습니다.
- 세 window의 late side output에서 같은 event가 중복 DLQ로 갈 가능성을 제거했습니다.
- window alert ID는 late update로 sample event가 바뀌어도 안정적으로 유지됩니다.

### 14. 전달 보장과 규칙 설정 주입

- 환경변수 static field에 의존하던 위험 임계값을 직렬화 가능한 `RiskRuleConfig`로 바꿔
  JobManager에서 검증한 값이 TaskManager에 그대로 전달되게 했습니다.
- Kafka source isolation level을 명시하고 exactly-once에서 `read_committed`가 아니면
  시작을 거부합니다.
- sink guarantee와 Flink checkpoint mode를 작업 코드에서 같은 모드로 맞춥니다.

### 15. Replay 정책 대칭성

- Flink parser와 replay helper가 같은 미래 시각, 식별자 타입, schema version 범위를
  적용합니다.
- 잘못된 미래 event가 replay와 DLQ 사이를 반복하거나 불명확한 실행 ID가 audit key에
  들어가는 것을 차단합니다.

### 16. API 계약과 실패 응답

- Pydantic request의 알 수 없는 field를 거부하고 query limit/timeout에 상한을 둡니다.
- 선택된 DLQ record는 중간 offset을 전부 훑지 않고 각 exact offset에서 직접 읽습니다.
- Kafka client 오류는 내부 예외를 노출하지 않는 일관된 `503` 응답으로 변환합니다.

### 17. CDC 등록 경로

- Kafka Connect `PUT /config` endpoint에 잘못된 wrapper JSON을 보내던 문제를
  평면 connector config로 수정했습니다.
- 등록 도구가 connector와 모든 task의 `RUNNING` 상태를 확인하고 실패 trace를
  보고하도록 보강했습니다.

### 18. 로컬·Kubernetes 운영 경계

- Compose port를 `127.0.0.1`에만 바인딩하고 Kafka/PostgreSQL data volume과 PostgreSQL
  readiness를 추가했습니다.
- exactly-once K8s overlay의 API consumer도 `read_committed`를 사용하며, prod-like API는
  token Secret을 필수로 요구합니다.

### 19. Metric 의미와 CI 공백

- gauge에 `_total`을 붙이던 중복 metric을 제거하고 topic 합계는 PromQL로 계산합니다.
- 구성 topic 누락 alert, Ruff, at-least-once/exactly-once E2E matrix, CDC connector smoke를
  CI 품질 게이트에 추가했습니다.

### 20. 문서 정합성

- source isolation, window cleanup 기준, replay 미래 시각 정책, CDC PUT payload, named
  volume과 localhost 노출 경계를 실제 구현과 맞췄습니다.
- stable replay ID가 Kafka 발행 자체의 멱등성을 보장하지 않는다는 한계를 명시했습니다.

## 3차 세부 검토: 21–30

### 21. 저장소와 빌드 컨텍스트

- Ruff cache와 coverage 산출물을 Git 및 Docker build context에서 제외했습니다.
- Makefile의 Kafka Compose 명령이 공통 `COMPOSE` 설정을 재사용하도록 정리했습니다.

### 22. Python 설정 경계

- Generator와 Replayer도 공백 Kafka 주소·topic·consumer group을 시작 단계에서
  거부합니다.
- isolation level 정규화, Python 3.12 UTC 표현, immutable 상수 표현을 통일했습니다.

### 23. API HTTP 계약

- DLQ topic이 API allowlist에서 빠진 설정을 시작 단계에서 거부합니다.
- 선택 offset의 존재하지 않는 partition은 Kafka 연결 실패와 구분된 `404`로 반환하고,
  timeout은 시스템 시각 변경에 영향받지 않는 단조 시계로 계산합니다.
- Kafka 상세 오류는 서버 로그에만 남기고 클라이언트에는 일반화된 오류를 반환합니다.

### 24. DLQ 요약 경계

- 사용하지 않던 replay run ID 인자와 무의미한 summary UUID 생성을 제거했습니다.
- 빈 error type/reason 표시를 `UNKNOWN`으로 통일하고 음수 sample limit을 거부합니다.

### 25. Java parser 일관성

- 정상 snake_case CDC 필드가 있어도 잘못된 camelCase 별칭을 먼저 평가하던 eager
  fallback을 제거했습니다.
- CDC 문자열 타입, 음수 future-skew와 payment status 공백을 입력 경계에서 검증합니다.

### 26. 집계 의미와 결정성

- 가맹점 차원을 포함하는 실제 key에 맞게 aggregate type을
  `COUNTRY_CATEGORY_MERCHANT_1M`으로 수정했습니다.
- 집계 차원값을 trim해 공백 차이로 별도 key가 만들어지지 않도록 했습니다.

### 27. Compose 재실행 안전성

- 이미 같은 이름의 Flink job이 `RUNNING`이면 submit container가 중복 job을 제출하지
  않고 정상 종료합니다.
- Generator에 전달되지만 사용되지 않던 isolation 환경변수를 제거했습니다.

### 28. CDC와 도구 진단

- 사용자가 명시한 connector config 경로가 잘못되면 기본 파일로 조용히 대체하지 않고
  fail-fast 처리합니다.
- connector JSON 객체/class, finite API timeout과 replay deadline 계산을 보강했습니다.

### 29. CI와 경보 사각지대

- CDC smoke가 connector 상태뿐 아니라 실제 초기 snapshot Kafka 레코드까지 검증합니다.
- API metrics endpoint 자체가 scrape되지 않을 때를 `up` metric으로 감지합니다.

### 30. 문서 세부 정합성

- `cdc-up`과 `cdc-register`의 실제 책임, CDC enrichment와 watermark 적용 순서를 코드와
  맞췄습니다.
- 집계 type, CDC CI 검증 범위, API scrape 경보와 세 번째 검토 결과를 문서화했습니다.

## 검증 기준

변경 완료 후 다음을 함께 통과해야 합니다.

- Java unit test와 shaded JAR build
- Python helper/API contract test와 Ruff
- Python compile, shell syntax, JSON/Avro JSON 구문 검사
- Docker Compose 전체 profile 렌더링
- Prometheus config/rule 검사
- Kubernetes dev, exactly-once, prod-like overlay 렌더링
- Kafka → Flink → alert/aggregate/DLQ → replay Compose E2E를 at-least-once와
  exactly-once에서 각각 실행
- PostgreSQL → Debezium → Kafka connector/task와 초기 snapshot record smoke test

이 검증은 회귀를 줄이는 장치이지 성능, 보안, 고가용성, 데이터 정확성에 대한 production
인증은 아닙니다.
