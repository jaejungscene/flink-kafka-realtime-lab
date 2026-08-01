# Delivery Guarantee 실습

이 문서는 Flink Kafka sink의 `AT_LEAST_ONCE`와 `EXACTLY_ONCE`를 분리해서 실행하고 비교하는 방법을 설명합니다.

## 한 줄 요약

기본 실행은 `AT_LEAST_ONCE`입니다. 빠르고 단순하며 대부분의 알람/집계 lab에 적합합니다. `EXACTLY_ONCE`는 Kafka transaction과 Flink checkpoint를 함께 사용해 결과 topic에 중복 commit을 줄이는 실습 모드입니다.

## 실행 모드

| 모드 | 실행 | Flink checkpointing | Kafka sink | API consumer |
| --- | --- | --- | --- | --- |
| 기본 | `make up` | `AT_LEAST_ONCE` | `AT_LEAST_ONCE` | `read_uncommitted` |
| Exactly-once 실습 | `make up-exactly-once` | `EXACTLY_ONCE` | `EXACTLY_ONCE` | `read_committed` |

CI와 같은 흐름으로 검증하려면:

```bash
make ci-smoke
make ci-smoke-exactly-once
```

Kubernetes manifest를 비교하려면:

```bash
kubectl kustomize k8s/overlays/dev
kubectl kustomize k8s/overlays/exactly-once
```

## 코드에서 바뀌는 것

Flink job은 `--sinkDeliveryGuarantee` 인자를 받습니다.

```bash
--sinkDeliveryGuarantee AT_LEAST_ONCE
--sinkDeliveryGuarantee EXACTLY_ONCE
--sourceIsolationLevel read_committed
```

`EXACTLY_ONCE`일 때는 Kafka sink에 operator별 transactional id prefix를 설정합니다. 같은 topic으로 여러 sink가 쓰더라도 alert, aggregate, DLQ sink가 서로 다른 transaction namespace를 사용합니다.
Job은 sink guarantee에 맞춰 Flink checkpoint mode도 명시적으로 설정하며,
`EXACTLY_ONCE`와 `read_uncommitted` 조합은 시작 단계에서 거부합니다.

## 실무에서 주의할 점

- Exactly-once는 “Flink가 Kafka output topic에 commit하는 범위”의 보장입니다.
- Kafka consumer가 transaction 결과만 보려면 `isolation.level=read_committed`가 필요합니다.
- 외부 HTTP 호출, DB upsert, 알림 발송 같은 sink까지 자동으로 exactly-once가 되지는 않습니다.
- producer가 같은 business event나 DLQ offset을 다시 발행하는 중복은 delivery guarantee가
  제거하지 않습니다. `eventId`/`replayId` 기반 dedup 정책이 별도로 필요합니다.
- checkpoint interval이 너무 길면 transaction이 오래 열리고, 너무 짧으면 overhead가 늘어납니다.
- 운영 Kafka는 `transaction.state.log.replication.factor`, `transaction.state.log.min.isr`, `transaction.max.timeout.ms`를 broker 수와 checkpoint 정책에 맞게 설정해야 합니다.
- 장애 복구 후 중복이 절대 없어야 하는 도메인은 output key, idempotent sink, dedup key도 함께 설계해야 합니다.
- Flink checkpoint mode, Kafka sink guarantee, source consumer isolation level을 함께
  바꿔야 합니다. 이 저장소는 잘못된 조합을 시작 단계에서 검증합니다.

## 이 프로젝트의 의도

이 lab은 기본값을 보수적으로 `AT_LEAST_ONCE`로 둡니다. 학습자는 먼저 window, DLQ, replay, late event를 이해하고, 그 다음 `EXACTLY_ONCE` 모드로 바꾸어 checkpoint와 Kafka transaction이 어떤 추가 조건을 요구하는지 확인할 수 있습니다.
