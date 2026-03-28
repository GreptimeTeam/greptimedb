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

-- Zero-key reduction should rewrite SinglePartitioned to Single.
SELECT sum(m)
FROM (
  SELECT a, b, c, min(val) AS m
  FROM reduce_aggregate_repartition
  GROUP BY a, b, c
) s;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
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
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE
SELECT b, sum(val)
FROM reduce_aggregate_repartition_non_subset
GROUP BY b;

DROP TABLE reduce_aggregate_repartition_non_subset;
