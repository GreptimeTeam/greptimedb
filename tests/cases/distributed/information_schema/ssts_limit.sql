CREATE TABLE ssts_limit_case (
  a INT PRIMARY KEY INVERTED INDEX,
  b STRING SKIPPING INDEX,
  c STRING FULLTEXT INDEX,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO ssts_limit_case VALUES
  (500, 'a', 'a', 1),
  (1500, 'b', 'b', 2),
  (2500, 'c', 'c', 3);

ADMIN FLUSH_TABLE('ssts_limit_case');

SELECT COUNT(DISTINCT node_id) > 1 AS has_multi_datanodes
FROM information_schema.ssts_manifest;

SELECT COUNT(*) AS limited_rows
FROM (
  SELECT region_id
  FROM information_schema.ssts_manifest
  LIMIT 1
);

SELECT COUNT(*) AS filtered_limited_rows
FROM (
  SELECT region_id
  FROM information_schema.ssts_manifest
  WHERE region_id > 0
  LIMIT 1
);

-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT COUNT(*) AS filtered_limited_rows
FROM (
  SELECT region_id
  FROM information_schema.ssts_manifest
  WHERE table_id > 0
  LIMIT 1
);

-- The frontend orders results from all datanodes before applying the global offset.
SELECT min_ts, max_ts
FROM information_schema.ssts_manifest
WHERE region_id > 0
ORDER BY min_ts DESC
LIMIT 2 OFFSET 1;

-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT min_ts
FROM information_schema.ssts_manifest
WHERE region_id > 0
ORDER BY min_ts DESC
LIMIT 1;

-- Timestamp filtering happens before global ordering and offset. The sort key
-- must remain available even when it is absent from the output projection.
SELECT max_ts
FROM information_schema.ssts_manifest
WHERE region_id > 0 AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
ORDER BY min_ts DESC
LIMIT 1 OFFSET 1;

-- Each datanode contributes aggregate states; the frontend merges them.
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT COUNT(*) AS files, COUNT(1) AS files_one, SUM(file_size) AS bytes,
  COUNT(*) FILTER (WHERE file_size >= 1) AS nonempty_files,
  SUM(file_size) FILTER (WHERE file_size = 0) AS zero_bytes
FROM information_schema.ssts_manifest
WHERE table_id > 0 AND region_id > 0
  AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
  AND max_ts < '1970-01-01T00:00:00.004Z'::TIMESTAMP_NS;

-- Compare exact byte totals with a limited-input query that must not push
-- aggregation through its global limit. The limit exceeds this fixture's size.
WITH pushed AS (
  SELECT COUNT(*) AS files, COUNT(1) AS files_one, SUM(file_size) AS bytes,
    COUNT(*) FILTER (WHERE file_size >= 1) AS nonempty_files,
    SUM(file_size) FILTER (WHERE file_size >= 1) AS nonempty_bytes,
    COUNT(*) FILTER (WHERE file_size = 0) AS zero_files,
    SUM(file_size) FILTER (WHERE file_size = 0) AS zero_bytes
  FROM information_schema.ssts_manifest
  WHERE table_id > 0 AND region_id > 0
    AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
    AND max_ts < '1970-01-01T00:00:00.004Z'::TIMESTAMP_NS
), fallback AS (
  SELECT COUNT(*) AS files, SUM(file_size) AS bytes
  FROM (
    SELECT file_size FROM information_schema.ssts_manifest
    WHERE table_id > 0 AND region_id > 0
      AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
      AND max_ts < '1970-01-01T00:00:00.004Z'::TIMESTAMP_NS
    LIMIT 1000
  )
)
SELECT pushed.files, pushed.files_one, pushed.nonempty_files, pushed.zero_files,
  pushed.files = fallback.files AS same_count,
  pushed.bytes = fallback.bytes AS same_bytes,
  pushed.nonempty_bytes = fallback.bytes AS same_filtered_bytes,
  pushed.zero_bytes IS NULL AS empty_filtered_sum
FROM pushed CROSS JOIN fallback;

-- All nodes have empty input: COUNT returns zero, while SUM remains NULL.
SELECT COUNT(*) AS files, COUNT(1) AS files_one, SUM(file_size) AS bytes,
  COUNT(*) FILTER (WHERE file_size >= 1) AS nonempty_files,
  SUM(file_size) FILTER (WHERE file_size >= 1) AS nonempty_bytes
FROM information_schema.ssts_manifest WHERE region_id = 0;

-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT COUNT(*), SUM(file_size)
FROM (SELECT file_size FROM information_schema.ssts_manifest LIMIT 1);

SELECT COUNT(*) AS files, SUM(file_size) > 0 AS has_bytes
FROM (SELECT file_size FROM information_schema.ssts_manifest LIMIT 1);

-- Other aggregate shapes retain ordinary query execution.
SELECT level, COUNT(*) AS files, SUM(file_size) > 0 AS has_bytes
FROM information_schema.ssts_manifest GROUP BY level ORDER BY level;

SELECT COUNT(DISTINCT region_id) AS distinct_regions,
  SUM(file_size + 1) = SUM(file_size) + COUNT(*) AS complex_input_matches
FROM information_schema.ssts_manifest;

-- Non-file-size columns and general aggregate filters use the same state merge.
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
EXPLAIN SELECT COUNT(file_id), SUM(num_rows), SUM(level), SUM(table_id),
  SUM(CAST(num_row_groups AS DOUBLE)), SUM(CAST(max_row_group_uncompressed_size AS DECIMAL(20, 0))),
  COUNT(index_file_size), SUM(index_file_size),
  COUNT(*) FILTER (WHERE visible AND level = 0)
FROM information_schema.ssts_manifest WHERE region_id > 0;

WITH pushed AS (
  SELECT COUNT(file_id) AS files, COUNT(index_file_size) AS indexes,
    SUM(num_rows) AS rows, SUM(level) AS levels, SUM(table_id) AS tables,
    SUM(index_file_size) AS index_bytes,
    SUM(CAST(num_row_groups AS DOUBLE)) AS float_groups,
    SUM(CAST(max_row_group_uncompressed_size AS DECIMAL(20, 0))) AS decimal_bytes,
    COUNT(*) FILTER (WHERE visible AND level = 0) AS visible_files,
    SUM(num_rows) FILTER (WHERE min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS) AS recent_rows
  FROM information_schema.ssts_manifest WHERE region_id > 0
), fallback AS (
  SELECT COUNT(file_id) AS files, COUNT(index_file_size) AS indexes,
    SUM(num_rows) AS rows, SUM(level) AS levels, SUM(table_id) AS tables,
    SUM(index_file_size) AS index_bytes,
    SUM(CAST(num_row_groups AS DOUBLE)) AS float_groups,
    SUM(CAST(max_row_group_uncompressed_size AS DECIMAL(20, 0))) AS decimal_bytes,
    COUNT(*) FILTER (WHERE visible AND level = 0) AS visible_files,
    SUM(num_rows) FILTER (WHERE min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS) AS recent_rows
  FROM (SELECT * FROM information_schema.ssts_manifest WHERE region_id > 0 LIMIT 1000)
)
SELECT pushed.files = fallback.files AS same_count,
  pushed.indexes = fallback.indexes AS same_nullable_count,
  pushed.rows = fallback.rows AS same_rows,
  pushed.float_groups = fallback.float_groups AS same_float_groups,
  pushed.decimal_bytes = fallback.decimal_bytes AS same_decimal_bytes,
  pushed.levels = fallback.levels AS same_levels,
  pushed.tables = fallback.tables AS same_tables,
  pushed.index_bytes IS NOT DISTINCT FROM fallback.index_bytes AS same_index_bytes,
  pushed.visible_files = fallback.visible_files AS same_visible_count,
  pushed.recent_rows IS NOT DISTINCT FROM fallback.recent_rows AS same_recent_rows
FROM pushed CROSS JOIN fallback;

DROP TABLE ssts_limit_case;
