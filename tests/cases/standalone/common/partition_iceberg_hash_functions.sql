-- The SQL forms of the partition hash: measure a value, derive breakpoints, and
-- render a rule list that can be pasted into `PARTITION ON COLUMNS`.

SELECT iceberg_hash('iceberg'), iceberg_hash(34), iceberg_hash(NULL);

SELECT iceberg_hash_bounds(2), iceberg_hash_bounds(4);

SELECT iceberg_hash_breakpoints('iceberg', 'a', 'iceberg');

-- The column name is quoted, so a name that is not a plain lowercase identifier
-- can be pasted as-is.
SELECT iceberg_hash_ranges_sql('host-name', 4);

-- A column name in another Arrow string representation renders the same rules.
SELECT iceberg_hash_ranges_sql(arrow_cast('host-name', 'Utf8View'), 4);

-- The rules below are the exact output of
-- `iceberg_hash_ranges_sql('host-name', 4)` pasted into a table definition.
CREATE TABLE hash_function_rules (
  "host-name" STRING,
  ts TIMESTAMP TIME INDEX,
  v INT,
  PRIMARY KEY ("host-name")
)
PARTITION ON COLUMNS ("host-name") (
  iceberg_hash("host-name") < -1073741824,
  iceberg_hash("host-name") >= -1073741824 AND iceberg_hash("host-name") < 0,
  iceberg_hash("host-name") >= 0 AND iceberg_hash("host-name") < 1073741824,
  iceberg_hash("host-name") >= 1073741824
);

SHOW CREATE TABLE hash_function_rules;

SELECT partition_name, partition_description
FROM information_schema.partitions
WHERE table_name = 'hash_function_rules'
ORDER BY partition_name;

-- One value per bucket, plus NULL, which the first rule catches.
INSERT INTO hash_function_rules VALUES
  ('b', 1, 1),
  ('delta', 2, 2),
  ('a', 3, 3),
  ('iceberg', 4, 4),
  (NULL, 5, 5);

-- The hashes the rendered predicates compare against, computed by the function
-- the rules evaluate: b < -1073741824, -1073741824 <= delta < 0,
-- 0 <= a < 1073741824, iceberg >= 1073741824, NULL hashes to NULL.
SELECT "host-name", iceberg_hash("host-name") FROM hash_function_rules ORDER BY ts;

-- Row counts per rendered predicate, plus the NULL hash no comparison matches ...
SELECT
  sum(CASE WHEN iceberg_hash("host-name") < -1073741824 THEN 1 ELSE 0 END) AS p0,
  sum(CASE WHEN iceberg_hash("host-name") >= -1073741824 AND iceberg_hash("host-name") < 0 THEN 1 ELSE 0 END) AS p1,
  sum(CASE WHEN iceberg_hash("host-name") >= 0 AND iceberg_hash("host-name") < 1073741824 THEN 1 ELSE 0 END) AS p2,
  sum(CASE WHEN iceberg_hash("host-name") >= 1073741824 THEN 1 ELSE 0 END) AS p3,
  sum(CASE WHEN iceberg_hash("host-name") IS NULL THEN 1 ELSE 0 END) AS nulls
FROM hash_function_rules;

-- ... must match the rows each region actually stored: region 0 holds p0 plus
-- the NULL rows, and every other region holds exactly its bucket.
-- SQLNESS SLEEP 3s
SELECT region_number, region_rows
FROM information_schema.region_statistics
WHERE table_id IN (
  SELECT table_id FROM information_schema.tables
  WHERE table_name = 'hash_function_rules' AND table_schema = 'public'
)
ORDER BY region_number;

DROP TABLE hash_function_rules;
