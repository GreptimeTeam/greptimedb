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
