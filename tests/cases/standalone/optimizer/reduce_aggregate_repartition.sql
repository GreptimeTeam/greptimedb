CREATE TABLE reduce_aggregate_repartition (
  a STRING NULL,
  b STRING NULL,
  c STRING NULL,
  ts TIMESTAMP TIME INDEX,
  val BIGINT NULL,
  PRIMARY KEY (a, b, c)
) PARTITION ON COLUMNS (a, b, c) (
  a < 'm',
  a >= 'm'
);

INSERT INTO reduce_aggregate_repartition VALUES
  ('a', 'x', 'u', 1000, 10),
  ('a', 'x', 'v', 2000, 5),
  ('a', 'y', 'u', 3000, 7),
  ('n', 'x', 'u', 4000, 4),
  ('n', 'x', 'v', 5000, 8),
  ('n', 'y', 'u', 6000, 3);

-- Keep the outer aggregate on `sum` so this case isolates
-- ReduceAggregateRepartition rather than the min/max combiner rule.
SELECT a, b, sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s
GROUP BY b, a
ORDER BY a, b;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN VERBOSE
SELECT a, b, sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s
GROUP BY b, a;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT a, b, sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s
GROUP BY b, a;

-- Another grouped SQL reduction case to keep the rule coverage centered on SQL,
-- not just TQL/PromQL planning.
SELECT a, count(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s
GROUP BY a
ORDER BY a;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT a, count(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s
GROUP BY a;

-- Zero-key reduction should rewrite SinglePartitioned to Single.
SELECT sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s;

DROP TABLE reduce_aggregate_repartition;

CREATE TABLE reduce_aggregate_repartition_non_subset (
  a STRING NULL,
  b STRING NULL,
  ts TIMESTAMP TIME INDEX,
  val BIGINT NULL,
  PRIMARY KEY (a, b)
) PARTITION ON COLUMNS (a) (
  a < 'm',
  a >= 'm'
);

INSERT INTO reduce_aggregate_repartition_non_subset VALUES
  ('a', 'x', 1000, 10),
  ('n', 'x', 2000, 5),
  ('a', 'y', 3000, 7),
  ('n', 'y', 4000, 3);

-- Changing the group key from `a` to `b` is not a reduction, so the
-- repartition must remain in place.
SELECT b, sum(val)
FROM reduce_aggregate_repartition_non_subset
GROUP BY b
ORDER BY b;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT b, sum(val)
FROM reduce_aggregate_repartition_non_subset
GROUP BY b;

DROP TABLE reduce_aggregate_repartition_non_subset;

CREATE TABLE reduce_aggregate_repartition_metric (
  ts TIMESTAMP(3) TIME INDEX,
  job STRING,
  instance STRING,
  greptime_value DOUBLE,
  PRIMARY KEY(job, instance),
);

INSERT INTO reduce_aggregate_repartition_metric VALUES
  (0, 'job1', 'instance1', 1),
  (0, 'job1', 'instance2', 2),
  (0, 'job2', 'instance1', 3),
  (5000, 'job1', 'instance1', 4),
  (5000, 'job1', 'instance2', 5),
  (5000, 'job2', 'instance1', 6),
  (10000, 'job1', 'instance1', 7),
  (10000, 'job1', 'instance2', 8),
  (10000, 'job2', 'instance1', 9);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s')
count(count(reduce_aggregate_repartition_metric) by (job));

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s')
count(count(rate(reduce_aggregate_repartition_metric[5s])) by (job));

DROP TABLE reduce_aggregate_repartition_metric;
