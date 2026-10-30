DESC TABLE information_schema.ssts_manifest;

DESC TABLE information_schema.ssts_storage;

DESC TABLE information_schema.ssts_index_meta;

CREATE TABLE sst_case (
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

INSERT INTO sst_case VALUES
  (500, 'a', 'a', 1),
  (1500, 'b', 'b', 2),
  (2500, 'c', 'c', 3);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_index_meta ORDER BY meta_json;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

INSERT INTO sst_case VALUES
  (24, 'foo', 'foo', 100),
  (124, 'bar', 'bar', 200),
  (1024, 'baz', 'baz', 300);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by region_id, sequence;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_index_meta ORDER BY meta_json;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

-- Regression: repeated physical optimization must preserve the global DISTINCT limit.
SELECT COUNT(*) AS distinct_limited_rows
FROM (
  SELECT DISTINCT region_id
  FROM information_schema.ssts_manifest
  LIMIT 1
);

-- Timestamp filters must run before ordering and limiting. Sort columns must
-- remain available even when they are absent from the output projection.
SELECT min_ts, max_ts
FROM information_schema.ssts_manifest
WHERE region_id > 0 AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
ORDER BY min_ts ASC NULLS LAST, max_ts ASC
LIMIT 2;

SELECT min_ts, max_ts
FROM information_schema.ssts_manifest
WHERE region_id > 0
ORDER BY max_ts DESC NULLS FIRST, min_ts DESC
LIMIT 2 OFFSET 1;

SELECT visible
FROM information_schema.ssts_manifest
WHERE region_id > 0 AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
ORDER BY min_ts DESC
LIMIT 2;

-- Table predicates share region metadata pruning, including when table_id
-- is not part of the output projection.
SELECT min_ts, max_ts FROM information_schema.ssts_manifest
WHERE table_id > 0 ORDER BY min_ts, max_ts LIMIT 2;

SELECT min_ts FROM information_schema.ssts_manifest
WHERE table_id = 0 ORDER BY min_ts LIMIT 1;

-- A false region predicate and a zero limit must produce empty results.
SELECT min_ts FROM information_schema.ssts_manifest
WHERE region_id = 0 ORDER BY min_ts LIMIT 1;

SELECT min_ts FROM information_schema.ssts_manifest
ORDER BY min_ts LIMIT 0;

-- COUNT-only output still applies predicates on columns absent from the output.
SELECT COUNT(*) AS files
FROM information_schema.ssts_manifest
WHERE table_id > 0
  AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
  AND max_ts < '1970-01-01T00:00:00.004Z'::TIMESTAMP_NS;

-- Standalone executes these aggregates normally; distributed mode may merge
-- remote states. Both preserve COUNT/SUM and FILTER semantics.
SELECT COUNT(*) AS files, COUNT(1) AS files_one,
  SUM(file_size) > 0 AS has_bytes,
  COUNT(*) FILTER (WHERE file_size >= 1) AS nonempty_files,
  COUNT(*) FILTER (WHERE file_size = 0) AS zero_files,
  SUM(file_size) FILTER (WHERE file_size = 0) AS zero_bytes
FROM information_schema.ssts_manifest
WHERE table_id > 0 AND region_id > 0
  AND min_ts >= '1970-01-01T00:00:00.002Z'::TIMESTAMP_NS
  AND max_ts < '1970-01-01T00:00:00.004Z'::TIMESTAMP_NS;

SELECT COUNT(*) AS files, COUNT(1) AS files_one, SUM(file_size) AS bytes
FROM information_schema.ssts_manifest WHERE region_id = 0;

-- Generic column aggregates retain the same semantics in both deployment modes.
SELECT COUNT(file_id) = COUNT(*) AS same_file_count,
  COUNT(index_file_size) <= COUNT(*) AS nullable_count,
  SUM(num_rows) > 0 AS has_rows,
  SUM(CAST(num_row_groups AS DOUBLE)) > 0 AS has_float_groups,
  SUM(CAST(max_row_group_uncompressed_size AS DECIMAL(20, 0))) >= 0 AS has_decimal_bytes,
  COUNT(*) FILTER (WHERE visible AND level = 0) <= COUNT(*) AS visible_count
FROM information_schema.ssts_manifest WHERE region_id > 0;

SELECT COUNT(index_file_size), SUM(num_rows), SUM(level), SUM(index_file_size)
FROM information_schema.ssts_manifest WHERE region_id = 0;

DROP TABLE sst_case;
