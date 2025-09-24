CREATE TABLE IF NOT EXISTS aggr_optimize_not (
  a STRING NULL,
  b STRING NULL,
  c STRING NULL,
  d STRING NULL,
  greptime_timestamp TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (greptime_timestamp),
  PRIMARY KEY (a, b, c, d)
) PARTITION ON COLUMNS (a, b, c) (a < 'b', a >= 'b',);

-- Case 0: group by columns are the same as partition columns.
-- This query shouldn't push down aggregation even if group by columns are partitioned.
-- because sort is already pushed down.
-- If it does, it will cause a wrong result.
-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') max by (a, b, c) (max_over_time(aggr_optimize_not [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') max by (a, b, c) (max_over_time(aggr_optimize_not [2m]));

-- Case 1: group by columns are prefix of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') sum by (a, b) (max_over_time(aggr_optimize_not [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') sum by (a, b) (max_over_time(aggr_optimize_not [2m]));

-- Case 2: group by columns are prefix of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') avg by (a) (max_over_time(aggr_optimize_not [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') avg by (a) (max_over_time(aggr_optimize_not [2m]));

-- Case 3: group by columns are superset of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') count by (a, b, c, d) (max_over_time(aggr_optimize_not [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') count by (a, b, c, d) (max_over_time(aggr_optimize_not [2m]));

-- Case 4: group by columns are not prefix of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') min by (b, c, d) (max_over_time(aggr_optimize_not [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') min by (b, c, d) (max_over_time(aggr_optimize_not [2m]));

-- Case 5: a simple sum
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain sum(aggr_optimize_not);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze sum(aggr_optimize_not);

-- TODO(discord9): more cases for aggr push down interacting with partitioning&tql
CREATE TABLE IF NOT EXISTS aggr_optimize_not_count (
  a STRING NULL,
  b STRING NULL,
  c STRING NULL,
  d STRING NULL,
  greptime_timestamp TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (greptime_timestamp),
  PRIMARY KEY (a, b, c, d)
) PARTITION ON COLUMNS (a, b, c) (a < 'b', a >= 'b',);

-- Case 6: Test average rate (sum/count like)
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') sum by (a, b, c) (rate(aggr_optimize_not [2m])) / sum by (a, b, c) (rate(aggr_optimize_not_count [2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') sum by (a, b, c) (rate(aggr_optimize_not [2m])) / sum by (a, b, c) (rate(aggr_optimize_not_count [2m]));

-- Case 7: aggregate without sort should be pushed down. This one push down for include all partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b,
  c;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b,
  c;

-- Case 8: aggregate without sort should be pushed down. This one push down for include all partition columns then some
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b,
  c,
  d;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b,
  c,
  d;

-- Case 9: aggregate without sort should be pushed down. This one push down for step aggr push down.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT
  min(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b;

-- Case 10: aggregate without sort should be pushed down. This one push down for step aggr push down with complex aggr
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN
SELECT
  min(greptime_value) + max(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT
  min(greptime_value) + max(greptime_value)
FROM
  aggr_optimize_not
GROUP BY
  a,
  b;


-- Case 11: aggregate with subquery
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN
SELECT
  a,
  min(greptime_value)
FROM
  (
    SELECT
      a,
      b,
      greptime_value
    FROM
      aggr_optimize_not
    ORDER BY
      a,
      b
  )
GROUP BY
  a;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT
  a,
  min(greptime_value)
FROM
  (
    SELECT
      a,
      b,
      greptime_value
    FROM
      aggr_optimize_not
    ORDER BY
      a,
      b
  )
GROUP BY
  a;

drop table aggr_optimize_not_count;

drop table aggr_optimize_not;

--
-- Additional test cases for step aggregation pushdown
--
CREATE TABLE step_aggr_extended (
    pk_col_1 STRING,
    pk_col_2 BIGINT,
    val_col_1 BIGINT,
    val_col_2 STRING,
    val_col_3 BIGINT,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(pk_col_1, pk_col_2)
) PARTITION ON COLUMNS (pk_col_1) (
    pk_col_1 < 'f',
    pk_col_1 >= 'f'
);

INSERT INTO step_aggr_extended VALUES
    ('a', 1, 100, 'v1', 10, 1672531200000),
    ('a', 2, 200, 'v2', NULL, 1672531201000),
    ('g', 1, 300, 'v1', 30, 1672531202000),
    ('g', 2, 400, 'v2', 40, 1672531203000),
    ('a', 3, 100, 'v3', 10, 1672531204000),
    ('g', 3, 300, 'v3', 30, 1672531205000),
    ('h', 4, 500, NULL, 50, 1672531206000);


-- Case 12: GROUP BY includes a mix of partition key and non-partition key.
-- `pk_col_1` is a partition key, `pk_col_2` is not.
-- This should pushdown entire aggregation to datanodes since it's partitioned by `pk_col_1`.
-- Expected: Full pushdown of aggregation to datanodes.
SELECT pk_col_1, pk_col_2, sum(val_col_1) FROM step_aggr_extended GROUP BY pk_col_1, pk_col_2 ORDER BY pk_col_1, pk_col_2;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT pk_col_1, pk_col_2, sum(val_col_1) FROM step_aggr_extended GROUP BY pk_col_1, pk_col_2 ORDER BY pk_col_1, pk_col_2;

-- Case 13: COUNT(DISTINCT) aggregation.
-- `DISTINCT` aggregation is more complex and requires a two-phase distinct calculation in a distributed environment. Currently not supported for pushdown.
-- Expected: datanode only do table scan, actual aggregation happens on frontend.
SELECT COUNT(DISTINCT val_col_1) FROM step_aggr_extended;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT COUNT(DISTINCT val_col_1) FROM step_aggr_extended;

-- Case 14: Aggregation with a HAVING clause.
-- The `HAVING` clause filters results after aggregation.
-- Expected: The `HAVING` filter should be applied on the frontend after the final aggregation is complete, not pushed down to datanodes.
SELECT pk_col_2, sum(val_col_1) FROM step_aggr_extended GROUP BY pk_col_2 HAVING sum(val_col_1) > 300 ORDER BY pk_col_2;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT pk_col_2, sum(val_col_1) FROM step_aggr_extended GROUP BY pk_col_2 HAVING sum(val_col_1) > 300 ORDER BY pk_col_2;

-- Case 15: Aggregation on a column with NULL values.
-- `SUM` should ignore NULLs. `COUNT(val_col_2)` should count non-nulls, `COUNT(*)` should count all rows.
-- Expected: Correct aggregation results, proving NULLs are handled properly in a distributed context.
SELECT SUM(val_col_3), COUNT(val_col_2), COUNT(val_col_3), COUNT(*) FROM step_aggr_extended;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT SUM(val_col_3), COUNT(val_col_2), COUNT(val_col_3), COUNT(*) FROM step_aggr_extended;

-- Case 16: Aggregation on STRING columns.
-- `MIN` and `MAX` can operate on strings.
-- Expected: Correct lexicographical min/max results.
SELECT MIN(pk_col_1), MAX(val_col_2) FROM step_aggr_extended;

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT MIN(pk_col_1), MAX(val_col_2) FROM step_aggr_extended;

-- Case 17: Aggregation on an empty input set.
-- `WHERE` clause filters out all rows.
-- Expected: Aggregation should return correct default values (e.g., COUNT is 0, SUM is NULL).
SELECT SUM(val_col_1), COUNT(*) FROM step_aggr_extended WHERE pk_col_1 = 'non_existent';

-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT SUM(val_col_1), COUNT(*) FROM step_aggr_extended WHERE pk_col_1 = 'non_existent';

DROP TABLE step_aggr_extended;
