# DLQ Replay 가이드

이 프로젝트는 파싱/검증 실패와 late event를 `transactions.dlq`로 분리합니다. 자동
replay는 원본 의미를 추론하지 않아도 되는 일부 `PARSE_OR_VALIDATION_ERROR`에만
허용합니다.

## 재처리 토픽을 분리한 이유

DLQ record를 raw topic에 바로 다시 넣으면 event lineage를 설명하기 어려워집니다. 별도 replay topic을 두면 Flink job은 복구된 record를 다시 처리하면서도, 해당 record가 remediation 과정을 거쳤다는 사실을 보존할 수 있습니다.

## 로컬 흐름

API로 원인 요약과 replay 미리보기를 먼저 확인합니다.

```bash
make dlq-summary
make dlq-replay-preview
make dlq-replay-api
make consume-replay
```

CLI tool container로 같은 replay 흐름을 실행할 수도 있습니다.

```bash
make produce
make consume-dlq
make replay-dlq
make consume-replay
```

CLI replayer는 시작할 때 `REPLAY_RUN_ID`, topic 충돌, isolation level, 처리 건수와 미래
시각 허용 범위를 한 번에 검증합니다. `REPLAY_RUN_ID`는 재실행해도 동일하게 유지해야 같은
DLQ offset에서 안정적인 `replayId`가 생성됩니다.

Flink job은 `transactions.raw`와 `transactions.replay`를 모두 소비합니다.

API endpoint 상세는 [DLQ Summary/Replay API 실습](dlq-replay-api-guide.md)을 참고합니다.

## 자동 replay 허용 범위

- `rawValue`가 유효한 JSON object입니다.
- `userId`, 양수 epoch millis `eventTime`, 유한한 음수 아닌 `amount`가 보존되어 있습니다.
- `eventTime`은 현재 시각보다 설정된 미래 허용 범위를 넘지 않습니다.
- `mlFraudScore`와 `ipRisk`가 있으면 각각 `0..1`, `0..100` 범위입니다.
- 식별자는 문자열이고 `schemaVersion`은 양의 정수입니다.
- 비어 있는 `eventId`는 source DLQ topic/partition/offset으로 결정적으로 생성합니다.
- replay run과 source offset metadata를 추가합니다.

음수 금액, 누락된 사용자, 잘못된 event time은 자동으로 고치지 않습니다. `LATE_EVENT`와
`REFERENCE_DATA_PARSE_ERROR`도 별도 backfill/remediation 경로가 필요합니다.

## 운영 패턴

- DLQ는 immutable하게 보존합니다.
- 허용 정책을 통과하고 replay metadata가 붙은 record는 replay topic에 씁니다.
- operator, ticket id, source DLQ offset, replay timestamp 같은 metadata를 추가합니다.
- Replay 도구에는 access control과 audit log를 둡니다.
- 같은 source offset을 다시 발행할 수 있으므로 downstream은 `eventId`/`replayId` 기반
  dedup 또는 처리 이력 저장소를 별도로 설계합니다.
