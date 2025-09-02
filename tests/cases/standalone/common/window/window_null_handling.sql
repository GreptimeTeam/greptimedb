-- Migrated from DuckDB test: Multiple window tests with NULL handling
-- Tests window functions with NULL values

CREATE TABLE null_test("id" INTEGER, val INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO null_test VALUES 
(1, 100, 1000), (2, NULL, 2000), (3, 300, 3000), (4, NULL, 4000), (5, 500, 5000);

-- Window functions with NULL values
SELECT "id", val, 
  ROW_NUMBER() OVER (ORDER BY val NULLS LAST) as row_num,
  RANK() OVER (ORDER BY val NULLS LAST) as rank_val
FROM null_test ORDER BY "id";

-- LEAD/LAG with NULL values
SELECT "id", val,
  LAG(val, 1) OVER (ORDER BY "id") as prev_val,
  LEAD(val, 1) OVER (ORDER BY "id") as next_val
FROM null_test ORDER BY "id";

-- Aggregate window functions with NULL
SELECT "id", val,
  SUM(val) OVER (ORDER BY "id" ROWS UNBOUNDED PRECEDING) as running_sum,
  COUNT(val) OVER (ORDER BY "id" ROWS UNBOUNDED PRECEDING) as running_count
FROM null_test ORDER BY "id";

-- FIRST_VALUE/LAST_VALUE with NULL
SELECT "id", val,
  FIRST_VALUE(val) OVER (ORDER BY "id" ROWS UNBOUNDED PRECEDING) as first_val,
  LAST_VALUE(val) OVER (ORDER BY "id" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_val
FROM null_test ORDER BY "id";

DROP TABLE null_test;
