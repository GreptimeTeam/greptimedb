-- Query indexed JSON paths alongside ordinary columns in flat SSTs.
CREATE TABLE json2_skipping_query (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        max_auto_expanded_paths = 0,
        service.name STRING SKIPPING INDEX WITH (granularity='1'),
        "service.name" STRING SKIPPING INDEX WITH (granularity='1'),
        status BIGINT SKIPPING INDEX WITH (granularity='1'),
        success BOOLEAN SKIPPING INDEX WITH (granularity='1'),
        duration DOUBLE SKIPPING INDEX WITH (granularity='1'),
        bytes BIGINT UNSIGNED SKIPPING INDEX WITH (granularity='1'),
        unindexed STRING
    ),
    ordinary STRING SKIPPING INDEX WITH (granularity='1')
) WITH ('append_mode' = 'true', 'sst_format' = 'flat');

INSERT INTO json2_skipping_query VALUES
    (1, '{"service":{"name":"api"},"service.name":"literal","status":200,"success":true,"duration":1.5,"bytes":18446744073709551615,"unindexed":"x"}', 'a'),
    (2, '{"service":{"name":"worker"},"service.name":"api","status":500,"success":false,"duration":2.5,"bytes":42,"unindexed":"y"}', 'b'),
    (3, '{}', 'c'), (4, NULL, 'd');

ADMIN FLUSH_TABLE('json2_skipping_query');

-- Nested and literal dotted paths resolve to different targets.
SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'api' ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE json_get(attrs, '$."service.name"') = 'api' ORDER BY ts;

-- Integer predicates support equality, reversed ranges, BETWEEN, and IN.
SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT = 200 ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE 400 < attrs.status::BIGINT ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT BETWEEN 100 AND 300 ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT IN (200, 500) ORDER BY ts;

-- OR predicates work within one path and across different paths.
SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'api' OR attrs.service.name = 'worker' ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'api' OR json_get(attrs, '$."service.name"') = 'api' ORDER BY ts;

-- Regex, boolean, floating-point, and unsigned predicates return matching rows.
SELECT ts FROM json2_skipping_query WHERE attrs.service.name ~ '^a.*[ip]$' ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.success::BOOLEAN = true ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.duration::DOUBLE > 2.0 ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.bytes::BIGINT UNSIGNED = 18446744073709551615::BIGINT UNSIGNED ORDER BY ts;

-- Ordinary and JSON index predicates can be combined.
SELECT ts FROM json2_skipping_query WHERE ordinary = 'b' AND attrs.status::BIGINT > 400 ORDER BY ts;

-- Missing paths and null roots remain visible to IS NULL.
SELECT ts FROM json2_skipping_query WHERE attrs.service.name IS NULL ORDER BY ts;

-- Unindexed hints remain queryable.
SELECT ts FROM json2_skipping_query WHERE attrs.unindexed = 'x' ORDER BY ts;

-- Implicit comparison and explicit conversion preserve query results.
SELECT ts FROM json2_skipping_query WHERE attrs.status = '200' ORDER BY ts;

SELECT ts FROM json2_skipping_query WHERE attrs.duration::BIGINT = 1 ORDER BY ts;

-- Verify equality and IN actually prune, including warm-cache reads.
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_bloom_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
-- SQLNESS REPLACE "max_part_range":\s*\{[^}]*\} "max_part_range":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'absent';

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_bloom_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
-- SQLNESS REPLACE "max_part_range":\s*\{[^}]*\} "max_part_range":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT IN (900, 901);

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_bloom_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
-- SQLNESS REPLACE "max_part_range":\s*\{[^}]*\} "max_part_range":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'absent';

INSERT INTO json2_skipping_query VALUES (5, '{"service":{"name":"api"},"status":201}', 'e');
ADMIN FLUSH_TABLE('json2_skipping_query');
ADMIN COMPACT_TABLE('json2_skipping_query', 'swcs', '86400');
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_bloom_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
-- SQLNESS REPLACE "max_part_range":\s*\{[^}]*\} "max_part_range":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'absent';
SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'api' ORDER BY ts;
SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT IN (200, 201, 500) ORDER BY ts;

-- Disable hints' indexes: results remain identical across existing SSTs.
ALTER TABLE json2_skipping_query MODIFY COLUMN attrs JSON2 (max_auto_expanded_paths = 0, service.name STRING, "service.name" STRING, status BIGINT, success BOOLEAN, duration DOUBLE, bytes BIGINT UNSIGNED, unindexed STRING);
SELECT ts FROM json2_skipping_query WHERE attrs.service.name = 'api' ORDER BY ts;
SELECT ts FROM json2_skipping_query WHERE attrs.status::BIGINT IN (200, 201, 500) ORDER BY ts;
DROP TABLE json2_skipping_query;

-- Old SST without a path index, new SST with both index kinds, then a type change.
CREATE TABLE json2_skipping_evolution (ts TIMESTAMP TIME INDEX, j JSON2 (n BIGINT)) WITH ('append_mode'='true');
INSERT INTO json2_skipping_evolution VALUES (1, '{"n":42}');
ADMIN FLUSH_TABLE('json2_skipping_evolution');
ALTER TABLE json2_skipping_evolution MODIFY COLUMN j JSON2 (n BIGINT SKIPPING INDEX WITH (granularity='1') INVERTED INDEX);
SHOW CREATE TABLE json2_skipping_evolution;
INSERT INTO json2_skipping_evolution VALUES (2, '{"n":42}');
ADMIN FLUSH_TABLE('json2_skipping_evolution');
SELECT ts FROM json2_skipping_evolution WHERE json_get(j, '$.n') = 42 ORDER BY ts;
ALTER TABLE json2_skipping_evolution MODIFY COLUMN j JSON2 (n DOUBLE SKIPPING INDEX WITH (granularity='1'));
INSERT INTO json2_skipping_evolution VALUES (3, '{"n":42.0}');
ADMIN FLUSH_TABLE('json2_skipping_evolution');
SELECT ts FROM json2_skipping_evolution WHERE json_get(j, '$.n') = 42.0 ORDER BY ts;
-- SQLNESS ARG restart=true
SHOW CREATE TABLE json2_skipping_evolution;
SELECT ts FROM json2_skipping_evolution WHERE json_get(j, '$.n') = 42.0 ORDER BY ts;
ADMIN COMPACT_TABLE('json2_skipping_evolution', 'swcs', '86400');
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_bloom_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
-- SQLNESS REPLACE "max_part_range":\s*\{[^}]*\} "max_part_range":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_skipping_evolution WHERE json_get(j, '$.n') = 99.0;
SELECT ts FROM json2_skipping_evolution WHERE json_get(j, '$.n') = 42.0 ORDER BY ts;
DROP TABLE json2_skipping_evolution;
