CREATE TABLE promql_column_pruning (
  ts TIMESTAMP(3) TIME INDEX,
  job STRING,
  instance STRING,
  region STRING,
  greptime_value DOUBLE,
  PRIMARY KEY(job, instance, region),
);

INSERT INTO promql_column_pruning VALUES
  (0, 'job1', 'instance1', 'region1', 1),
  (0, 'job1', 'instance2', 'region1', 2),
  (0, 'job2', 'instance1', 'region1', 3),
  (5000, 'job1', 'instance1', 'region1', 4),
  (5000, 'job1', 'instance2', 'region1', 5),
  (5000, 'job2', 'instance1', 'region1', 6),
  (10000, 'job1', 'instance1', 'region1', 7),
  (10000, 'job1', 'instance2', 'region1', 8),
  (10000, 'job2', 'instance1', 'region1', 9);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') sum(promql_column_pruning);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') sum(rate(promql_column_pruning[5s]));

DROP TABLE promql_column_pruning;
