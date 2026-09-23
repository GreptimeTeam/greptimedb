-- Enabling a hint index must keep rows from SSTs with only an ordinary index.
CREATE TABLE json2_index_evolution (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (max_auto_expanded_paths = 0, value BIGINT),
    ordinary STRING INVERTED INDEX
) WITH ('append_mode' = 'true', 'sst_format' = 'flat', 'index.inverted_index.segment_row_count' = '1');

INSERT INTO json2_index_evolution VALUES (1, '{"value":42}', 'a');

ADMIN FLUSH_TABLE('json2_index_evolution');

ALTER TABLE json2_index_evolution MODIFY COLUMN attrs JSON2 (max_auto_expanded_paths = 0, value BIGINT INVERTED INDEX);

SHOW CREATE TABLE json2_index_evolution;

SELECT ts FROM json2_index_evolution WHERE json_get(attrs, '$.value', NULL::BIGINT) = 42 ORDER BY ts;

INSERT INTO json2_index_evolution VALUES (2, '{"value":42}', 'b');

ADMIN FLUSH_TABLE('json2_index_evolution');

SELECT ts FROM json2_index_evolution WHERE json_get(attrs, '$.value', NULL::BIGINT) = 42 ORDER BY ts;

-- Changing the hint type must not use old indexes with a different value encoding.
ALTER TABLE json2_index_evolution MODIFY COLUMN attrs JSON2 (max_auto_expanded_paths = 0, value DOUBLE INVERTED INDEX);

SHOW CREATE TABLE json2_index_evolution;

INSERT INTO json2_index_evolution VALUES (3, '{"value":42.5}', 'c');

ADMIN FLUSH_TABLE('json2_index_evolution');

SELECT target_type, target_json, COUNT(*) AS index_count
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_evolution')
GROUP BY target_type, target_json ORDER BY target_type, target_json;

SELECT ts FROM json2_index_evolution WHERE json_get(attrs, '$.value', NULL::DOUBLE) >= 42.0 ORDER BY ts;

DROP TABLE json2_index_evolution;

-- Removing a hint preserves old JSON values and leaves its old index unused.
CREATE TABLE json2_index_schema (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (max_auto_expanded_paths = 0, value BIGINT INVERTED INDEX),
    ordinary STRING INVERTED INDEX
) WITH ('append_mode' = 'true', 'sst_format' = 'flat', 'index.inverted_index.segment_row_count' = '1');

INSERT INTO json2_index_schema VALUES (1, '{"value":42}', 'a'), (2, '{"value":7}', 'b');

ADMIN FLUSH_TABLE('json2_index_schema');

SELECT ts FROM json2_index_schema WHERE json_get(attrs, '$.value', NULL::BIGINT) = 42 ORDER BY ts;

ALTER TABLE json2_index_schema MODIFY COLUMN attrs JSON2 (max_auto_expanded_paths = 0);

SHOW CREATE TABLE json2_index_schema;

SELECT target_type, COUNT(*) AS index_count
FROM information_schema.ssts_index_meta
WHERE table_id = (SELECT table_id FROM information_schema.tables WHERE table_name = 'json2_index_schema')
GROUP BY target_type ORDER BY target_type;

INSERT INTO json2_index_schema VALUES (3, '{"value":42}', 'c');

ADMIN FLUSH_TABLE('json2_index_schema');

SELECT ts FROM json2_index_schema WHERE json_get(attrs, '$.value', NULL::BIGINT) = 42 ORDER BY ts;

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_inverted_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
-- SQLNESS REPLACE "metrics_per_partition":\s*\[.*metrics= "metrics_per_partition":REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE ,\s"dyn_filters":\s\[.* REDACTED
-- SQLNESS REPLACE "index_size":\d+ "index_size":REDACTED
-- SQLNESS REPLACE "files":\s*\[.*\],\s*"flat_format" "files":REDACTED,"flat_format"
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_index_schema WHERE json_get(attrs, '$.value', NULL::BIGINT) > 600;

DROP TABLE json2_index_schema;
