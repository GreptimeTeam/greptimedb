-- Corresponding to issue #7913.
-- Verify a filter over a projected millisecond cast of a non-ms time index
-- is passed down to scan as a casted time-index predicate for pruning.

CREATE TABLE cast_time_index_filter_pushdown (
    ts TIMESTAMP_NS NOT NULL TIME INDEX,
    val BIGINT,
) ENGINE = mito
WITH
    (append_mode = 'true', sst_format = 'flat');

INSERT INTO cast_time_index_filter_pushdown VALUES
    ('2023-06-12 01:04:49.999999999'::TIMESTAMP_NS, 1),
    ('2023-06-12 01:04:50.000000123'::TIMESTAMP_NS, 2),
    ('2023-06-12 01:04:50.999999999'::TIMESTAMP_NS, 3),
    ('2023-06-12 01:04:51.000000000'::TIMESTAMP_NS, 4);

ADMIN FLUSH_TABLE ('cast_time_index_filter_pushdown');

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
-- SQLNESS REPLACE "flat_format":\s\w+, "flat_format": REDACTED,
-- SQLNESS REPLACE (files.*) REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT ts_ms, val
FROM (
    SELECT ts::TIMESTAMP_MS AS ts_ms, val
    FROM cast_time_index_filter_pushdown
) projected
WHERE ts_ms = '2023-06-12 01:04:50'::TIMESTAMP_MS
ORDER BY val;

SELECT ts_ms, val
FROM (
    SELECT ts::TIMESTAMP_MS AS ts_ms, val
    FROM cast_time_index_filter_pushdown
) projected
WHERE ts_ms = '2023-06-12 01:04:50'::TIMESTAMP_MS
ORDER BY val;

DROP TABLE cast_time_index_filter_pushdown;
