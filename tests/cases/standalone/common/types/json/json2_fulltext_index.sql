-- Compare JSON paths with an ordinary column using the same fulltext backend.
CREATE TABLE json2_fulltext_bloom (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (max_auto_expanded_paths=0,
        nested.message STRING FULLTEXT INDEX WITH (backend='bloom', analyzer='English', case_sensitive='false', granularity='1') SKIPPING INDEX INVERTED INDEX,
        "nested.message" STRING FULLTEXT INDEX WITH (backend='bloom', analyzer='English', case_sensitive='false', granularity='1')),
    ordinary STRING FULLTEXT INDEX WITH (backend='bloom', analyzer='English', case_sensitive='false', granularity='1')
) WITH ('append_mode'='true', 'sst_format'='flat');
SHOW CREATE TABLE json2_fulltext_bloom;
INSERT INTO json2_fulltext_bloom VALUES
    (1, '{"nested":{"message":"error timeout"},"nested.message":"healthy"}', 'error timeout'),
    (2, '{"nested":{"message":"healthy"},"nested.message":"error timeout"}', 'healthy'),
    (3, '{"nested":{"message":"ERROR retry"}}', 'ERROR retry'),
    (4, '{}', NULL), (5, NULL, NULL),
    (6, '{"nested":{"message":""}}', '');
ADMIN FLUSH_TABLE('json2_fulltext_bloom');
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(ordinary, 'error') ORDER BY ts;
-- Combine fulltext predicates with inverted/skipping equality predicates.
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'error') AND attrs.nested.message = 'error timeout' ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(json_get(attrs, '$."nested.message"'), 'error') AND attrs.nested.message = 'healthy' ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(json_get(attrs, '$."nested.message"'), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE attrs.nested.message IS NULL ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'error') OR matches_term(json_get(attrs, '$."nested.message"'), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(lower(attrs.nested.message), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE attrs.nested.message LIKE '%error timeout%' ORDER BY ts;
SELECT ts FROM json2_fulltext_bloom WHERE ordinary LIKE '%error timeout%' ORDER BY ts;

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'absenttoken');

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'absenttoken');
INSERT INTO json2_fulltext_bloom VALUES (7, '{"nested":{"message":"error extra"}}', 'error extra');
ADMIN FLUSH_TABLE('json2_fulltext_bloom');
ADMIN COMPACT_TABLE('json2_fulltext_bloom', 'swcs', '86400');
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'error') ORDER BY ts;
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'absenttoken');
ALTER TABLE json2_fulltext_bloom MODIFY COLUMN attrs JSON2(max_auto_expanded_paths=0, nested.message STRING, "nested.message" STRING);
SELECT ts FROM json2_fulltext_bloom WHERE matches_term(attrs.nested.message, 'error') ORDER BY ts;
DROP TABLE json2_fulltext_bloom;

-- Compare JSON paths with an ordinary column using the same fulltext backend.
CREATE TABLE json2_fulltext_tantivy (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (max_auto_expanded_paths=0,
        nested.message STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='English', case_sensitive='false') SKIPPING INDEX INVERTED INDEX,
        "nested.message" STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='English', case_sensitive='false')),
    ordinary STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='English', case_sensitive='false')
) WITH ('append_mode'='true', 'sst_format'='flat');
SHOW CREATE TABLE json2_fulltext_tantivy;
INSERT INTO json2_fulltext_tantivy VALUES
    (1, '{"nested":{"message":"error timeout"},"nested.message":"healthy"}', 'error timeout'),
    (2, '{"nested":{"message":"healthy"},"nested.message":"error timeout"}', 'healthy'),
    (3, '{"nested":{"message":"ERROR retry"}}', 'ERROR retry'),
    (4, '{}', NULL), (5, NULL, NULL),
    (6, '{"nested":{"message":""}}', '');
ADMIN FLUSH_TABLE('json2_fulltext_tantivy');
SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_tantivy WHERE matches(ordinary, 'error') ORDER BY ts;
-- Combine fulltext predicates with inverted/skipping equality predicates.
SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'error') AND attrs.nested.message = 'error timeout' ORDER BY ts;
SELECT ts FROM json2_fulltext_tantivy WHERE matches(json_get(attrs, '$."nested.message"'), 'error') AND attrs.nested.message = 'healthy' ORDER BY ts;
SELECT ts FROM json2_fulltext_tantivy WHERE matches(json_get(attrs, '$."nested.message"'), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_tantivy WHERE attrs.nested.message IS NULL ORDER BY ts;
SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'error') OR matches(json_get(attrs, '$."nested.message"'), 'error') ORDER BY ts;

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'absenttoken');

-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'absenttoken');
INSERT INTO json2_fulltext_tantivy VALUES (7, '{"nested":{"message":"error extra"}}', 'error extra');
ADMIN FLUSH_TABLE('json2_fulltext_tantivy');
ADMIN COMPACT_TABLE('json2_fulltext_tantivy', 'swcs', '86400');
SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'error') ORDER BY ts;
-- SQLNESS REPLACE "metrics_per_partition":.*("rows_fulltext_filtered":\d+).*metrics= "metrics_per_partition":{$1} metrics=
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
EXPLAIN ANALYZE VERBOSE SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'absenttoken');
ALTER TABLE json2_fulltext_tantivy MODIFY COLUMN attrs JSON2(max_auto_expanded_paths=0, nested.message STRING, "nested.message" STRING);
SELECT ts FROM json2_fulltext_tantivy WHERE matches(attrs.nested.message, 'error') ORDER BY ts;
DROP TABLE json2_fulltext_tantivy;

CREATE TABLE json2_fulltext_chinese_bloom (
    ts TIMESTAMP TIME INDEX,
    j JSON2(text STRING FULLTEXT INDEX WITH (backend='bloom', analyzer='Chinese', case_sensitive='true', granularity='1')),
    ordinary STRING FULLTEXT INDEX WITH (backend='bloom', analyzer='Chinese', case_sensitive='true', granularity='1')
) WITH ('append_mode'='true');
INSERT INTO json2_fulltext_chinese_bloom VALUES
    (1, '{"text":"错误 日志 ERROR"}', '错误 日志 ERROR'),
    (2, '{"text":"正常 日志 error"}', '正常 日志 error');
ADMIN FLUSH_TABLE('json2_fulltext_chinese_bloom');
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(j.text, '错误') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(ordinary, '错误') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(j.text, 'ERROR') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(ordinary, 'ERROR') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(lower(j.text), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_bloom WHERE matches_term(lower(ordinary), 'error') ORDER BY ts;
DROP TABLE json2_fulltext_chinese_bloom;

CREATE TABLE json2_fulltext_chinese_tantivy (
    ts TIMESTAMP TIME INDEX,
    j JSON2(text STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='Chinese', case_sensitive='true')),
    ordinary STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='Chinese', case_sensitive='true')
) WITH ('append_mode'='true');
INSERT INTO json2_fulltext_chinese_tantivy VALUES
    (1, '{"text":"错误 日志 ERROR"}', '错误 日志 ERROR'),
    (2, '{"text":"正常 日志 error"}', '正常 日志 error');
ADMIN FLUSH_TABLE('json2_fulltext_chinese_tantivy');
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches(j.text, '错误') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches(ordinary, '错误') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches(j.text, 'ERROR') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches(ordinary, 'ERROR') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches_term(lower(j.text), 'error') ORDER BY ts;
SELECT ts FROM json2_fulltext_chinese_tantivy WHERE matches_term(lower(ordinary), 'error') ORDER BY ts;
DROP TABLE json2_fulltext_chinese_tantivy;

-- Old SSTs, hint changes, restart, and backend changes.
CREATE TABLE json2_fulltext_evolution (ts TIMESTAMP TIME INDEX, j JSON2(max_auto_expanded_paths=0)) WITH ('append_mode'='true');
INSERT INTO json2_fulltext_evolution VALUES (1, '{"message":"error old"}');
ADMIN FLUSH_TABLE('json2_fulltext_evolution');
ALTER TABLE json2_fulltext_evolution MODIFY COLUMN j JSON2(max_auto_expanded_paths=0, message STRING FULLTEXT INDEX WITH (granularity='1'));
INSERT INTO json2_fulltext_evolution VALUES (2, '{"message":"error new"}');
ADMIN FLUSH_TABLE('json2_fulltext_evolution');
SELECT ts FROM json2_fulltext_evolution WHERE matches_term(j.message, 'error') ORDER BY ts;
ALTER TABLE json2_fulltext_evolution MODIFY COLUMN j JSON2(max_auto_expanded_paths=0, message STRING FULLTEXT INDEX WITH (backend='tantivy', analyzer='Chinese', case_sensitive='true'));
INSERT INTO json2_fulltext_evolution VALUES (3, '{"message":"错误 日志 error"}');
ADMIN FLUSH_TABLE('json2_fulltext_evolution');
SELECT ts FROM json2_fulltext_evolution WHERE matches_term(j.message, 'error') ORDER BY ts;
-- SQLNESS ARG restart=true
SHOW CREATE TABLE json2_fulltext_evolution;
SELECT ts FROM json2_fulltext_evolution WHERE matches_term(j.message, 'error') ORDER BY ts;
ADMIN COMPACT_TABLE('json2_fulltext_evolution', 'swcs', '86400');
SELECT ts FROM json2_fulltext_evolution WHERE matches_term(j.message, 'error') ORDER BY ts;
ALTER TABLE json2_fulltext_evolution MODIFY COLUMN j JSON2(max_auto_expanded_paths=0, message BIGINT);
SELECT COUNT(*) FROM json2_fulltext_evolution;
DROP TABLE json2_fulltext_evolution;
