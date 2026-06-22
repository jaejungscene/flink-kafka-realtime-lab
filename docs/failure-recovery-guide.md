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
make produce-high-load
make lag
```

확인할 것:

- partition 수와 Flink parallelism이 처리량에 어떤 영향을 주는지
- lag가 증가해도 일정 시간이 지나면 회복되는지
- DLQ가 같이 증가한다면 producer schema나 validation 실패를 의심해야 하는지

## Savepoint

```bash
make savepoint
```

Savepoint는 job upgrade, rule 변경, 버전 배포 전에 상태를 안전하게 넘기기 위한 운영 절차입니다. 이 lab에서는 로컬 `/tmp/flink-savepoints`를 사용하지만, 운영에서는 S3, GCS, HDFS 같은 durable storage를 사용해야 합니다.
