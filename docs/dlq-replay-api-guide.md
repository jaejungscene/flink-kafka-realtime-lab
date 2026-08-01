# DLQ Summary/Replay API 실습

이 문서는 DLQ를 API로 요약하고, 자동 replay 정책을 통과한 record를 별도 topic으로
발행하는 흐름을 설명합니다.

## 왜 API로 분리하나

운영에서는 DLQ를 바로 재처리하지 않습니다. 먼저 원인별 규모를 보고, replay 가능한 record인지 확인하고, 작은 단위로 실행한 뒤 결과 topic을 검증합니다.

이 프로젝트는 그 흐름을 세 단계로 나눕니다.

| 단계 | 명령 | 목적 |
| --- | --- | --- |
| 요약 | `make dlq-summary` | error type, reason, replay 가능 수 확인 |
| 미리보기 | `make dlq-replay-preview` | 실제 발행 없이 replay 대상 확인 |
| 실행 | `make dlq-replay-api` | preview에서 선택한 offset을 `transactions.replay`로 발행 |

## API Endpoint

API는 기본적으로 `METRIC_TOPICS`와 같은 allowlist의 topic만 노출합니다.
`READABLE_TOPICS`로 목록을 좁히거나 바꿀 수 있습니다. `API_TOKEN`이 비어 있으면 로컬
인증은 꺼져 있고, 값이 있으면 아래 요청에 `X-API-Token` header가 필요합니다.

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

미리보기 응답의 `runId`와 `records[].partition/offset`을 검토한 뒤 같은 값을 실행
요청에 넣습니다. `confirm=true`가 없거나 선택한 offset이 없으면 실행되지 않습니다.

```bash
curl -X POST "http://localhost:8000/dlq/replay" \
  -H "content-type: application/json" \
  -d '{
    "max_messages": 1,
    "dry_run": false,
    "confirm": true,
    "replay_run_id": "api-preview-example",
    "records": [{"partition": 0, "offset": 42}]
  }'
```

`make dlq-replay-api`는 preview와 실행을 같은 run ID/offset으로 연결하므로 수동 JSON
조립보다 안전합니다.

실행 후 확인:

```bash
make consume-replay
curl "http://localhost:8000/topics/transactions.replay/messages?limit=10"
```

## Replay 허용 규칙

API와 CLI replayer는 같은 허용 정책을 사용합니다.

Replay 허용 판정과 metadata 정규화는 `common/python/realtime_lab/dlq_tools.py`에 있으며,
FastAPI와 CLI replayer가 같은 helper를 사용합니다.

- `PARSE_OR_VALIDATION_ERROR`만 자동 replay 후보로 취급
- 유효한 JSON object와 기존 `userId`, `eventTime`, `amount` 요구
- 비어 있는 `eventId`만 source offset 기반의 결정적 값으로 생성
- `eventTime`이 현재 시각보다 `MAX_FUTURE_SKEW_SECONDS` 이상 앞서면 차단
- 양의 `schemaVersion`과 문자열 식별자 요구
- `replayRunId`, `replaySourceTopic`, `replaySourcePartition`, `replaySourceOffset` 추가

malformed JSON, late event, reference data 오류, 음수/비정상 숫자는 자동 replay하지 않습니다.

## 실무 적용 포인트

- replay API는 기본값을 `dry_run=true`로 둡니다.
- 실행에는 preview에서 고른 exact offset, 동일한 run ID, `confirm=true`가 필요합니다.
- run ID는 3–80자의 영문자·숫자·점·밑줄·하이픈만 허용합니다.
- 한 번에 많은 record를 replay하지 말고 `max_messages`를 작게 시작합니다.
- DLQ 원본은 지우지 않고 immutable하게 보존합니다.
- replay topic을 raw topic과 분리해 lineage를 남깁니다.
- `API_TOKEN`을 설정하면 `X-API-Token` header가 topic/DLQ endpoint에 필요합니다.
- 이 lab은 영속적인 replay audit/dedup 저장소가 없습니다. 실제 운영에서는 ticket,
  승인자, 처리 결과, source offset unique constraint를 별도 저장해야 합니다.
- 동일 run ID와 offset은 안정적인 dedup key를 만들지만 Kafka 발행 자체를 멱등하게
  만들지는 않습니다. API retry 시 downstream dedup이 필요합니다.
