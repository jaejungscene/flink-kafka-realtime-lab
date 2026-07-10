# DLQ Summary/Replay API 실습

이 문서는 DLQ를 API로 요약하고, 보정 가능한 record를 replay topic으로 다시 발행하는 실습 흐름을 설명합니다.

## 왜 API로 분리하나

운영에서는 DLQ를 바로 재처리하지 않습니다. 먼저 원인별 규모를 보고, replay 가능한 record인지 확인하고, 작은 단위로 실행한 뒤 결과 topic을 검증합니다.

이 프로젝트는 그 흐름을 세 단계로 나눕니다.

| 단계 | 명령 | 목적 |
| --- | --- | --- |
| 요약 | `make dlq-summary` | error type, reason, replay 가능 수 확인 |
| 미리보기 | `make dlq-replay-preview` | 실제 발행 없이 replay 대상 확인 |
| 실행 | `make dlq-replay-api` | `transactions.replay`로 보정 record 발행 |

## API Endpoint

### DLQ 요약

```bash
curl "http://localhost:8000/dlq/summary?limit=100&from_beginning=true"
```

응답에서 볼 값:

- `scanned`: 스캔한 DLQ record 수
- `replayable`: replay 가능한 record 수
- `notReplayable`: replay 불가능한 record 수
- `byErrorType`: error type별 건수
- `byReason`: reason별 상위 건수
- `samples`: offset, reason, rawValue preview

### Replay 미리보기

```bash
curl -X POST "http://localhost:8000/dlq/replay" \
  -H "content-type: application/json" \
  -d '{"max_messages":5,"scan_limit":100,"dry_run":true}'
```

`dry_run=true`이면 Kafka에 발행하지 않습니다. 운영에서는 이 응답을 보고 ticket, 원인, 대상 offset을 확인한 뒤 실행합니다.

### Replay 실행

```bash
curl -X POST "http://localhost:8000/dlq/replay" \
  -H "content-type: application/json" \
  -d '{"max_messages":5,"scan_limit":100,"dry_run":false}'
```

실행 후 확인:

```bash
make consume-replay
curl "http://localhost:8000/topics/transactions.replay/messages?limit=10"
```

## Replay 보정 규칙

API replay는 CLI replayer와 같은 기준으로 보정합니다.

보정 로직은 `common/python/realtime_lab/dlq_tools.py`에 있으며, FastAPI와 CLI replayer가 같은 helper를 사용합니다.

- 누락된 `eventId`, `userId`, `merchantId`, `category` 채움
- 음수 `amount`를 `0` 이상으로 보정
- `eventTime`을 현재 시각으로 갱신
- `replayRunId`, `replaySourceTopic`, `replaySourcePartition`, `replaySourceOffset` 추가

malformed JSON처럼 원본 `rawValue` 자체를 파싱할 수 없는 record는 replay하지 않습니다.

## 실무 적용 포인트

- replay API는 기본값을 `dry_run=true`로 둡니다.
- 한 번에 많은 record를 replay하지 말고 `max_messages`를 작게 시작합니다.
- DLQ 원본은 지우지 않고 immutable하게 보존합니다.
- replay topic을 raw topic과 분리해 lineage를 남깁니다.
- 실제 운영에서는 인증, 권한, audit log, ticket id, 승인 workflow를 추가해야 합니다.
