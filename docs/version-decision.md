# Version Decision

## Chosen Baseline

- Kafka: `apache/kafka:4.1.2`
- Flink runtime and Maven dependencies: `2.1.2`
- Flink Kafka connector: `4.0.1-2.0`
- Java: `17`

## Why Kafka 4.1.2

Kafka 4.x is KRaft-only and represents the current direction after ZooKeeper removal. `4.1.2` is intentionally not the newest possible line, but it is newer than the initial 4.0 transition and fits a practical "stable but modern" portfolio baseline.

## Why Flink 2.1.2

Flink 2.x is the better investment for new learning and new project design. `2.1.2` is less aggressive than the newest 2.2 line while still teaching the 2.x ecosystem and migration mindset.

## Why Not Flink 1.20 LTS

Flink 1.20 is still useful for teams already operating 1.x jobs. This lab targets new learning and new builds, so it uses Flink 2.1.2 while documenting that many enterprises still run 1.x workloads.

## Connector Note

At the time this lab was updated, the public Maven Kafka connector line for Flink 2.x is `4.0.1-2.0`. The project pins that connector and validates it through the Maven test/build path.
