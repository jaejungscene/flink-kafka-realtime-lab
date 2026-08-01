# 버전 선택 근거

## 선택한 기준 버전

- Kafka: `apache/kafka:4.1.2`
- Flink runtime과 Maven dependency: `2.1.2`
- Flink Kafka connector: `4.0.1-2.0`
- Java: `17`

## 선택 원칙

이 저장소는 “현재 최신”을 문서에 주장하지 않고, 함께 검증한 image와 dependency를
고정합니다. 버전을 바꿀 때는 Java test, shaded JAR build, Compose E2E, connector와
operator 호환성을 다시 확인해야 합니다.

## Kafka 4.1.2

ZooKeeper 없이 KRaft controller/broker 구성을 실험하기 위해 Kafka 4.x image를
사용합니다. Compose는 단일 dual-role node이고 `prod-like` overlay는 3개 dual-role
node를 보여 주지만, 이것만으로 운영 고가용성이 검증되는 것은 아닙니다.

## Flink 2.1.2와 Java 17

DataStream API를 Flink 2.x runtime에서 검증하기 위해 runtime과 provided dependency를
`2.1.2`로 맞췄습니다. Maven enforcer는 build JDK를 Java 17로 제한해 local/CI/runtime
차이를 줄입니다. 기존 Flink 1.x workload의 migration 가능성을 뜻하지는 않습니다.

## 커넥터 참고 사항

Kafka connector는 `4.0.1-2.0`으로 고정합니다. Connector suffix와 Flink minor가 항상
동일하다는 가정을 하지 말고, 버전 변경 PR에서 공식 compatibility 정보와 E2E 결과를
함께 검토합니다.
