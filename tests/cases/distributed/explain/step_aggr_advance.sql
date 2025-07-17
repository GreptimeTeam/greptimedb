CREATE TABLE IF NOT EXISTS aggr_optimize_not (
  a STRING NULL,
  b STRING NULL,
  c STRING NULL,
  d STRING NULL,
  greptime_timestamp TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (greptime_timestamp),
  PRIMARY KEY (a, b, c, d)
)
PARTITION ON COLUMNS (a, b, c) (
      a < 'b',
      a >= 'b',
);

-- This query shouldn't push down aggregation even if group by columns are partitioned.
-- because sort is already pushed down.
-- If it does, it will cause a wrong result.
tql eval (1752591864, 1752592164, '30s') max by (a, b, c) (max_over_time(aggr_optimize_not[2m]));

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') max by (a, b, c) (max_over_time(aggr_optimize_not[2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') max by (a, b, c) (max_over_time(aggr_optimize_not[2m]));

-- Case 1: group by columns are prefix of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') sum by (a, b) (max_over_time(aggr_optimize_not[2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') sum by (a, b) (max_over_time(aggr_optimize_not[2m]));


-- Case 2: group by columns are prefix of partition columns.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
tql explain (1752591864, 1752592164, '30s') avg by (a) (max_over_time(aggr_optimize_not[2m]));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
tql analyze (1752591864, 1752592164, '30s') avg by (a) (max_over_time(aggr_optimize_not[2m]));

-- TODO(discord9): more cases for aggr push down interacting with partitioning&tql

drop table aggr_optimize_not;
