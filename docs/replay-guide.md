# DLQ Replay 가이드

이 프로젝트는 bad event와 late event를 `transactions.dlq`로 분리하고, 보정 가능한 record를 `transactions.replay`로 다시 발행하는 replayer를 제공합니다.

## 재처리 토픽을 분리한 이유

DLQ record를 raw topic에 바로 다시 넣으면 event lineage를 설명하기 어려워집니다. 별도 replay topic을 두면 Flink job은 복구된 record를 다시 처리하면서도, 해당 record가 remediation 과정을 거쳤다는 사실을 보존할 수 있습니다.

## 로컬 흐름

```bash
make produce
make consume-dlq
make replay-dlq
make consume-replay
```

Flink job은 `transactions.raw`와 `transactions.replay`를 모두 소비합니다.

## 재처리 도구가 보정하는 값

- 누락된 `eventId`
- 누락된 user, merchant, category, country, channel, device field
- 음수 amount
- 너무 오래된 event time

Replayer는 parse 자체가 불가능한 JSON까지 억지로 고치지는 않습니다. 실제 production에서도 일부 DLQ record는 사람의 판단이나 batch remediation이 필요합니다.

## 운영 패턴

- DLQ는 immutable하게 보존합니다.
- 보정된 record는 replay topic에 씁니다.
- operator, ticket id, source DLQ offset, replay timestamp 같은 metadata를 추가합니다.
- Replay 도구에는 access control과 audit log를 둡니다.
