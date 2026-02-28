-- Migrated from DuckDB test: Multiple advanced window function tests
-- Tests complex window function scenarios

CREATE TABLE window_data(
  group_id INTEGER,
  seq_num INTEGER, 
  "value" DOUBLE,
  "category" VARCHAR,
  ts TIMESTAMP TIME INDEX
);

INSERT INTO window_data VALUES 
(1, 1, 100.0, 'A', 1000), (1, 2, 150.0, 'A', 2000), (1, 3, 120.0, 'B', 3000),
(2, 1, 200.0, 'A', 4000), (2, 2, 180.0, 'B', 5000), (2, 3, 220.0, 'A', 6000);

-- Window with complex partitioning and ordering
SELECT 
  group_id, seq_num, "value", category,
  ROW_NUMBER() OVER (PARTITION BY group_id, category ORDER BY seq_num) as row_in_group_cat,
  DENSE_RANK() OVER (PARTITION BY group_id ORDER BY "value" DESC) as value_rank,
  LAG("value", 1, 0) OVER (PARTITION BY group_id ORDER BY seq_num) as prev_value
FROM window_data ORDER BY group_id, seq_num;

-- Running calculations with frames
SELECT 
  group_id, seq_num, "value",
  SUM("value") OVER (PARTITION BY group_id ORDER BY seq_num ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as rolling_sum_2,
  AVG("value") OVER (PARTITION BY group_id ORDER BY seq_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_avg,
  MAX("value") OVER (PARTITION BY group_id ORDER BY seq_num ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) as max_next_2
FROM window_data ORDER BY group_id, seq_num;

-- Percentage calculations
SELECT 
  group_id, "value", category,
  "value" / SUM("value") OVER (PARTITION BY group_id) * 100 as pct_of_group,
  PERCENT_RANK() OVER (ORDER BY "value") as percentile_rank,
  NTILE(3) OVER (ORDER BY "value") as tertile
FROM window_data ORDER BY "value";

DROP TABLE window_data;
