# 버전 선택 근거

## 선택한 기준 버전

- Kafka: `apache/kafka:4.1.2`
- Flink runtime과 Maven dependency: `2.1.2`
- Flink Kafka connector: `4.0.1-2.0`
- Java: `17`

## Kafka 4.1.2를 선택한 이유

Kafka 4.x는 KRaft-only 방향을 대표합니다. `4.1.2`는 무조건 최신 line을 따라가기보다는 Kafka 4.0의 KRaft-only 전환 이후 한 단계 안정화된 버전이라는 점에서, "현대적이지만 지나치게 공격적이지 않은" 실무형 baseline으로 적합합니다.

## Flink 2.1.2를 선택한 이유

Flink 2.x는 신규 학습과 신규 프로젝트 설계에 더 투자 가치가 높은 line입니다. `2.1.2`는 최신 2.2 line보다 조금 덜 공격적이면서도 Flink 2.x ecosystem과 migration mindset을 학습하기에 적합합니다.

## Flink 1.20 LTS를 쓰지 않은 이유

Flink 1.20은 이미 1.x job을 운영 중인 팀에는 여전히 의미가 있습니다. 하지만 이 lab은 신규 학습과 신규 구축을 목표로 하므로 Flink 2.1.2를 기준으로 삼았습니다. 동시에 많은 기업 운영계가 아직 1.x workload를 보유하고 있다는 점은 문서에서 명시합니다.

## 커넥터 참고 사항

이 프로젝트를 업데이트한 시점에 공개 Maven repository에서 확인 가능한 Flink 2.x Kafka connector line은 `4.0.1-2.0`입니다. 프로젝트는 해당 connector를 고정하고 Maven test/build 경로로 검증합니다.
