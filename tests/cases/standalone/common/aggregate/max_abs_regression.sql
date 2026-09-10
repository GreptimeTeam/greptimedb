-- Regression coverage for aggregate expressions with multiple MAX aggregates.
CREATE DATABASE max_abs_regression;

USE max_abs_regression;

CREATE TABLE t (
    ts TIMESTAMP TIME INDEX,
    v INT,
    PRIMARY KEY (v),
) ENGINE = mito;

INSERT INTO t VALUES
    ('2024-01-01 00:00:00', -11),
    ('2024-01-01 00:01:00', 3),
    ('2024-01-01 00:02:00', -7),
    ('2024-01-01 00:03:00', 5);

-- Expression aggregate regression.
SELECT MAX(ABS(v)), MAX(ts) FROM t;
SELECT MAX(ts), MAX(ABS(v)) FROM t;

-- Column-only multiple-MAX positive control.
SELECT MAX(v), MAX(ts) FROM t;

-- A mixed expression/column MAX must not send an incomplete dynamic filter to DN SeqScans.
-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (=Hash.*) =REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE ,\s"dyn_filters":\s\["DynamicFilter\s\[[^"]*\]"\] , "dyn_filters": ["DynamicFilter [ REDACTED ]"]
-- SQLNESS REPLACE metrics=REDACTED\s*\| metrics=REDACTED_|
EXPLAIN ANALYZE VERBOSE SELECT MAX(ABS(v)), MAX(ts) FROM t;

-- A column-only multiple-MAX query remains a dynamic-filter positive control.
-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (=Hash.*) =REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE ,\s"dyn_filters":\s\["DynamicFilter\s\[[^"]*\]"\] , "dyn_filters": ["DynamicFilter [ REDACTED ]"]
-- SQLNESS REPLACE metrics=REDACTED\s*\| metrics=REDACTED_|
EXPLAIN ANALYZE VERBOSE SELECT MAX(v), MAX(ts) FROM t;

DROP TABLE t;

USE public;

DROP DATABASE max_abs_regression;
