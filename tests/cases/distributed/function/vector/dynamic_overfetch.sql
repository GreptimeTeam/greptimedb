-- Part 1: Single-region (filter + tie-breaker)

CREATE TABLE vector_overfetch_dist (
  ts TIMESTAMP TIME INDEX,
  "id" INT PRIMARY KEY,
  "tag" STRING,
  v VECTOR(2) NOT NULL VECTOR INDEX WITH(engine = 'usearch', metric = 'l2sq')
);

INSERT INTO vector_overfetch_dist VALUES
('2024-01-01T00:00:00Z', 0, 'even', '[0.0, 0.0]'),
('2024-01-01T00:00:01Z', 1, 'odd',  '[1.0, 0.0]'),
('2024-01-01T00:00:02Z', 2, 'even', '[2.0, 0.0]'),
('2024-01-01T00:00:03Z', 3, 'odd',  '[3.0, 0.0]'),
('2024-01-01T00:00:04Z', 4, 'odd',  '[-1.0, 0.0]');

ADMIN FLUSH_TABLE('vector_overfetch_dist');

SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_dist
WHERE "tag" = 'odd'
ORDER BY d ASC, "id" ASC
LIMIT 2;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (SeqScan.*) REDACTED
-- SQLNESS REPLACE (ProjectionExec.*) REDACTED
-- SQLNESS REPLACE (CoalesceBatchesExec.*) REDACTED
-- SQLNESS REPLACE (FilterExec.*) REDACTED
-- SQLNESS REPLACE (CooperativeExec.*) REDACTED
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_dist
WHERE "tag" = 'odd'
ORDER BY d ASC, "id" ASC
LIMIT 2;

DROP TABLE vector_overfetch_dist;

-- Part 2: Multi-region (cross-partition filter + tie-breaker + high offset)

CREATE TABLE vector_overfetch_partitioned_dist (
  ts TIMESTAMP TIME INDEX,
  "id" INT PRIMARY KEY,
  "tag" STRING,
  v VECTOR(2) NOT NULL VECTOR INDEX WITH(engine = 'usearch', metric = 'l2sq')
) PARTITION ON COLUMNS ("tag") (
  "tag" < 'm',
  "tag" >= 'm'
);

INSERT INTO vector_overfetch_partitioned_dist VALUES
('2024-01-02T00:00:00Z', 1,  'a', '[0.0, 0.0]'),
('2024-01-02T00:00:01Z', 3,  'a', '[0.0, 0.0]'),
('2024-01-02T00:00:02Z', 5,  'a', '[0.0, 0.0]'),
('2024-01-02T00:00:03Z', 7,  'a', '[0.0, 0.0]'),
('2024-01-02T00:00:04Z', 9,  'a', '[0.0, 0.0]'),
('2024-01-02T00:00:05Z', 11, 'a', '[0.0, 0.0]'),
('2024-01-02T00:00:06Z', 13, 'a', '[0.0, 0.0]'),
('2024-01-02T00:00:07Z', 15, 'a', '[0.0, 0.0]'),
('2024-01-02T00:00:08Z', 101, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:09Z', 103, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:10Z', 105, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:11Z', 107, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:12Z', 109, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:13Z', 111, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:14Z', 113, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:15Z', 115, 'z', '[0.0, 0.0]'),
('2024-01-02T00:00:16Z', 102, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:17Z', 104, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:18Z', 106, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:19Z', 108, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:20Z', 110, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:21Z', 112, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:22Z', 114, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:23Z', 116, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:24Z', 118, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:25Z', 120, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:26Z', 122, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:27Z', 124, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:28Z', 126, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:29Z', 128, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:30Z', 130, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:31Z', 132, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:32Z', 134, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:33Z', 136, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:34Z', 138, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:35Z', 140, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:36Z', 142, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:37Z', 144, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:38Z', 146, 'z', '[1.0, 0.0]'),
('2024-01-02T00:00:39Z', 148, 'z', '[1.0, 0.0]');

ADMIN FLUSH_TABLE('vector_overfetch_partitioned_dist');

SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_partitioned_dist
WHERE "tag" IN ('a', 'z') AND ("id" % 2) = 0
ORDER BY d ASC, "id" ASC
LIMIT 2 OFFSET 20;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (CooperativeExec.*) REDACTED
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_partitioned_dist
WHERE "tag" IN ('a', 'z') AND ("id" % 2) = 0
ORDER BY d ASC, "id" ASC
LIMIT 2 OFFSET 20;

-- Retry scenario: initial k=2 fetches nearest d=0 rows (all odd IDs),
-- filter (id%2=0) eliminates them, forcing adaptive retry with larger k.
SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_partitioned_dist
WHERE "tag" IN ('a', 'z') AND ("id" % 2) = 0
ORDER BY d ASC, "id" ASC
LIMIT 2;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (CooperativeExec.*) REDACTED
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_partitioned_dist
WHERE "tag" IN ('a', 'z') AND ("id" % 2) = 0
ORDER BY d ASC, "id" ASC
LIMIT 2;

-- Edge case: offset beyond available data returns empty result.
SELECT
  "id",
  "tag",
  vec_l2sq_distance(v, '[0.0, 0.0]') AS d
FROM vector_overfetch_partitioned_dist
WHERE "tag" = 'z'
ORDER BY d ASC, "id" ASC
LIMIT 2 OFFSET 50;

DROP TABLE vector_overfetch_partitioned_dist;
