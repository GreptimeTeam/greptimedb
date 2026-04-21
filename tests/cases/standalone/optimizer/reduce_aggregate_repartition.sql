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

-- The source is already partitioned by (a, b, c), so the planner can satisfy
-- the outer aggregate's coarser (b, a) requirement at the MergeScan without a
-- repartition between the outer partial/final aggregate pair.
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

-- The same optimization should work when dropping both b and c from the
-- downstream partitioning requirement.
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

-- Changing the group key from a to b is not a coarsening of the existing
-- partition columns, so the repartition must remain.
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
