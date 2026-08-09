# 장애와 복구 실습 가이드

스트리밍 시스템은 정상 실행보다 장애 후 회복을 설명할 수 있어야 실무적입니다. 이 문서는 Docker Compose 환경에서 작은 장애를 일부러 만들고 확인하는 방법을 정리합니다.

## 사전 준비

```bash
make build
make up
make produce
make smoke
```

## TaskManager 장애

```bash
make chaos-kill-taskmanager
curl http://localhost:8081/jobs
make lag
```

확인할 것:

- Flink job이 재시작되는지
- consumer lag가 잠시 증가한 뒤 줄어드는지
- checkpoint 설정이 복구 시간에 어떤 영향을 주는지

## Kafka 재시작

```bash
make chaos-restart-kafka
docker compose ps
make topics
make lag
```

확인할 것:

- producer/consumer가 일시 실패 후 재연결되는지
- Flink job restart strategy가 동작하는지
- 단일 broker 구성의 한계가 무엇인지

## 부하 증가

```bash
make load-snapshot
make load-experiment-small
make lag
```

확인할 것:

- partition 수와 Flink parallelism이 처리량에 어떤 영향을 주는지
- lag가 증가해도 일정 시간이 지나면 회복되는지
- 특정 Flink vertex에서 backpressure가 높게 잡히는지
- DLQ가 같이 증가한다면 producer schema나 validation 실패를 의심해야 하는지

부하를 더 강하게 주려면:

```bash
LOAD_RUN_SECONDS=300 LOAD_EVENTS_PER_SECOND=250 make load-experiment
```

자세한 관측 순서는 [부하/백프레셔 실험 가이드](load-backpressure-guide.md)를 참고합니다.

## Savepoint

이 job은 source, 변환, window, sink에 고정된 operator UID를 사용합니다. 표시 이름이나 코드 순서를
바꿔도 UID가 유지되면 Flink가 savepoint의 상태를 같은 operator에 연결할 수 있습니다. 상태 구조를
의도적으로 바꾸는 경우에만 UID의 버전을 올리고, 배포 전에 기존 savepoint 복원 가능성을 검증합니다.

```bash
make savepoint
```

Savepoint는 job upgrade, rule 변경, 버전 배포 전에 상태를 넘기기 위한 운영 절차입니다.
이 lab의 기본 경로는 Compose named volume 안의
`file:///opt/flink/state/savepoints`입니다. 컨테이너 재생성에는 남지만 Docker host 장애나
`make down`의 volume 삭제는 견디지 못합니다. 운영에서는 Flink filesystem plugin과
인증을 갖춘 object storage 또는 HDFS를 사용해야 합니다.

로컬 savepoint 경로를 바꾸려면:

```bash
SAVEPOINT_DIR=file:///opt/flink/state/savepoints/manual make savepoint
```

## Delivery guarantee 비교

기본 모드는 `AT_LEAST_ONCE`입니다.

```bash
make down
make up
make produce
make smoke
```

Exactly-once 실습 모드는 Kafka transaction과 Flink checkpoint를 함께 사용합니다.

```bash
make down
make up-exactly-once
make produce
make smoke
```

실무에서는 exactly-once를 켜는 것만으로 충분하지 않습니다. 결과를 읽는 consumer의 `read_committed`, Kafka transaction 설정, 외부 sink의 idempotency를 함께 설계해야 합니다.
