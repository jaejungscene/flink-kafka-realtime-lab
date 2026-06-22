-- Flink SQL로 표현한 country/category/merchant 1분 집계 예시입니다.
-- 기본 실행 경로는 DataStream API이며, 이 파일은 같은 요구사항을 SQL로 어떻게 표현하는지 비교하기 위한 학습 자료입니다.

CREATE TABLE transactions_raw (
  eventId STRING,
  userId STRING,
  merchantId STRING,
  category STRING,
  eventTime BIGINT,
  amount DOUBLE,
  currency STRING,
  country STRING,
  channel STRING,
  deviceId STRING,
  mlFraudScore DOUBLE,
  paymentStatus STRING,
  ipRisk INT,
  event_ts AS TO_TIMESTAMP_LTZ(eventTime, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions.raw',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-realtime-lab',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE transactions_aggregates_sql (
  aggregateType STRING,
  `key` STRING,
  windowStart TIMESTAMP_LTZ(3),
  windowEnd TIMESTAMP_LTZ(3),
  eventCount BIGINT,
  totalAmount DOUBLE,
  avgAmount DOUBLE,
  avgFraudScore DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions.aggregates.sql',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO transactions_aggregates_sql
SELECT
  'COUNTRY_CATEGORY_MERCHANT_1M_SQL',
  CONCAT(COALESCE(country, 'UNKNOWN'), '|', COALESCE(category, 'uncategorized'), '|', COALESCE(merchantId, 'merchant-unknown')),
  window_start,
  window_end,
  COUNT(*),
  ROUND(SUM(amount), 2),
  ROUND(AVG(amount), 2),
  ROUND(AVG(mlFraudScore), 4)
FROM TABLE(
  TUMBLE(TABLE transactions_raw, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, country, category, merchantId;
