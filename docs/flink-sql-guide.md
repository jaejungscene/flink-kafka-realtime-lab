# Flink SQL 가이드

기본 job은 DataStream API로 구현되어 있습니다. `flink-sql/` 아래 SQL은 같은 집계 요구사항을 Flink SQL로 표현한 비교 예제입니다.

## 왜 SQL 예제를 두나

- 단순 집계, filtering, window는 SQL이 더 읽기 쉽습니다.
- 복잡한 state, side output, custom DLQ, rule class 테스트는 DataStream API가 더 명확합니다.
- 실무에서는 두 방식을 함께 쓰는 팀도 많습니다.

## 예제 파일

```text
flink-sql/country_category_merchant_aggregate.sql
```

이 SQL은 `transactions.raw`를 읽어서 `transactions.aggregates.sql`에 1분 window 집계를 씁니다.

## 비교 포인트

| 관점 | DataStream API | Flink SQL |
| --- | --- | --- |
| 복잡한 알람 rule | Java test와 함께 관리하기 좋음 | UDF가 필요할 수 있음 |
| window 집계 | 코드가 길어짐 | SQL이 간결함 |
| DLQ/side output | 표현력이 좋음 | 별도 설계가 필요함 |
| 데이터 분석가 협업 | 진입 장벽 있음 | 상대적으로 쉬움 |

## 실무 적용 팁

- 반복적인 집계는 SQL로 시작해도 좋습니다.
- 장애 처리, replay, custom validation이 중요하면 DataStream API를 고려합니다.
- SQL도 version control과 review 대상이어야 합니다.
