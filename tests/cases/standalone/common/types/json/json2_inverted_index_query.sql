CREATE TABLE json2_index_query (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        max_auto_expanded_paths = 0,
        service.name STRING INVERTED INDEX,
        "service.name" STRING INVERTED INDEX,
        status BIGINT INVERTED INDEX,
        success BOOLEAN INVERTED INDEX,
        duration DOUBLE INVERTED INDEX,
        bytes BIGINT UNSIGNED INVERTED INDEX,
        unindexed STRING
    ),
    ordinary STRING INVERTED INDEX
) WITH ('append_mode' = 'true', 'sst_format' = 'flat', 'index.inverted_index.segment_row_count' = '1');

INSERT INTO json2_index_query VALUES
    (1, '{"service":{"name":"api"},"service.name":"literal","status":200,"success":true,"duration":1.5,"bytes":18446744073709551615,"unindexed":"x"}', 'a'),
    (2, '{"service":{"name":"worker"},"service.name":"api","status":500,"success":false,"duration":2.5,"bytes":42,"unindexed":"y"}', 'b'),
    (3, '{}', 'c'), (4, NULL, 'd');

ADMIN FLUSH_TABLE('json2_index_query');

SELECT ts FROM json2_index_query WHERE attrs.service.name = 'api' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE json_get(attrs, '$."service.name"') = 'api' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.status::BIGINT = 200 ORDER BY ts;

SELECT ts FROM json2_index_query WHERE 400 < attrs.status::BIGINT ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.status::BIGINT BETWEEN 100 AND 300 ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.status::BIGINT IN (200, 500) ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.service.name = 'api' OR attrs.service.name = 'worker' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.service.name = 'api' OR json_get(attrs, '$."service.name"') = 'api' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.service.name ~ '^a.*[ip]$' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.success::BOOLEAN = true ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.duration::DOUBLE > 2.0 ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.bytes::BIGINT UNSIGNED = 18446744073709551615::BIGINT UNSIGNED ORDER BY ts;

SELECT ts FROM json2_index_query WHERE ordinary = 'b' AND attrs.status::BIGINT > 400 ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.service.name IS NULL ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.unindexed = 'x' ORDER BY ts;

-- Different output types must not use the hint's scalar encoding.
SELECT ts FROM json2_index_query WHERE attrs.status = '200' ORDER BY ts;

SELECT ts FROM json2_index_query WHERE attrs.duration::BIGINT = 1 ORDER BY ts;

-- Preserve index pruning counters while normalizing timing and execution details.
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_inverted_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_index_query WHERE attrs.service.name = 'absent';

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_inverted_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_index_query WHERE attrs.status::BIGINT > 600;

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_inverted_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_index_query WHERE attrs.service.name ~ '^z.*[ip]$';

INSERT INTO json2_index_query VALUES (5, '{"service":{"name":"api"},"status":201}', 'a');

ADMIN FLUSH_TABLE('json2_index_query');

ADMIN COMPACT_TABLE('json2_index_query', 'swcs', '86400');

SELECT ts FROM json2_index_query WHERE attrs.service.name = 'api' AND attrs.status::BIGINT < 300 ORDER BY ts;

DROP TABLE json2_index_query;

-- Without a JSON target, the ordinary-column blob must not cause false negatives.
CREATE TABLE json2_index_query_ignored (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (value STRING INVERTED INDEX),
    ordinary STRING INVERTED INDEX
) WITH ('append_mode' = 'true', 'index.inverted_index.ignore_column_ids' = '1');

INSERT INTO json2_index_query_ignored VALUES (1, '{"value":"yes"}', 'a');

ADMIN FLUSH_TABLE('json2_index_query_ignored');

SELECT ts FROM json2_index_query_ignored WHERE attrs.value = 'yes';

DROP TABLE json2_index_query_ignored;

-- JSON-only targets also work with the default SST format.
CREATE TABLE json2_index_query_only (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (value STRING INVERTED INDEX)
) WITH ('append_mode' = 'true', 'index.inverted_index.segment_row_count' = '1');

INSERT INTO json2_index_query_only VALUES (1, '{"value":"yes"}'), (2, '{"value":"no"}'), (3, NULL);

ADMIN FLUSH_TABLE('json2_index_query_only');

SELECT ts FROM json2_index_query_only WHERE attrs.value = 'yes';

SELECT ts FROM json2_index_query_only WHERE attrs.value = 'missing';

DROP TABLE json2_index_query_only;
