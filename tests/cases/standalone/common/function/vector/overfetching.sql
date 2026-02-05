-- Dynamic overfetching tests for vector index queries with filters and tie-breakers

CREATE TABLE vectors_overfetch (
    vec_id INT,
    group_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_overfetch VALUES
    (1, 1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, 1, '2024-01-01 00:00:01', '[1.0, 0.0]'),
    (3, 2, '2024-01-01 00:00:02', '[1.0, 0.0]'),
    (4, 1, '2024-01-01 00:00:03', '[0.0, 1.0]'),
    (5, 1, '2024-01-01 00:00:04', '[0.0, 1.0]'),
    (6, 2, '2024-01-01 00:00:05', '[0.0, 1.0]'),
    (7, 1, '2024-01-01 00:00:06', '[1.0, 1.0]'),
    (8, 1, '2024-01-01 00:00:07', '[1.0, 0.0]'),
    (9, 1, '2024-01-01 00:00:08', '[1.0, 0.0]'),
    (10, 1, '2024-01-01 00:00:09', '[1.0, 0.0]'),
    (11, 1, '2024-01-01 00:00:10', '[0.0, 1.0]'),
    (12, 1, '2024-01-01 00:00:11', '[0.0, 1.0]'),
    (13, 1, '2024-01-01 00:00:12', '[0.0, 1.0]'),
    (14, 2, '2024-01-01 00:00:13', '[1.0, 0.0]'),
    (15, 2, '2024-01-01 00:00:14', '[1.0, 0.0]'),
    (16, 2, '2024-01-01 00:00:15', '[0.0, 1.0]'),
    (17, 2, '2024-01-01 00:00:16', '[0.0, 1.0]'),
    (18, 1, '2024-01-01 00:00:17', '[1.0, 1.0]'),
    (19, 2, '2024-01-01 00:00:18', '[1.0, 1.0]'),
    (20, 1, '2024-01-01 00:00:19', '[0.5, 0.5]');

-- Memtable search with filter and tie-breaker
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE region_id=[^\s]+\([^)]*\) region_id=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- Memtable search with strict filter to trigger dynamic extend
-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE region_id=[^\s]+\([^)]*\) region_id=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_overfetch
WHERE group_id = 2
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- Strict filter result check with tie-breaker (strict filter triggers dynamic overfetching)
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 2
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- Strict filter with offset
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 2
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 2 OFFSET 3;

-- Filter with no matches (should return empty)
-- SQLNESS REPLACE \+[-\+]+\+(\n)\|\s*vec_id\s*\|\s*group_id\s*\|\s*distance\s*\|\n\+[-\+]+\+\n\+[-\+]+\+ ++$1++
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 99
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 3;

-- Memtable search with offset
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 2 OFFSET 1;

ADMIN FLUSH_TABLE('vectors_overfetch');

-- SST search with filter and tie-breaker
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE "(file_id|time_range_start|time_range_end)":"[^"]+" "$1":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE "(size|flat_format)":\s*(\d+|true|false) "$1":REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE region_id=[^\s]+\([^)]*\) region_id=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 5;

-- SST search with offset
SELECT vec_id, group_id, vec_l2sq_distance(embedding, '[1.0, 0.0]') AS distance
FROM vectors_overfetch
WHERE group_id = 1
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 2 OFFSET 1;

DROP TABLE vectors_overfetch;
