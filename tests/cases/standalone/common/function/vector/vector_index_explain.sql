-- Vector index explain analyze coverage


-- ============================================
-- Part 1: Single table KNN explain
-- ============================================
CREATE TABLE vectors_explain (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_explain VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

ADMIN FLUSH_TABLE('vectors_explain');

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE ("file_id":"[^"]+") "file_id":"REDACTED"
-- SQLNESS REPLACE ("time_range_start":"[^"]+") "time_range_start":"REDACTED"
-- SQLNESS REPLACE ("time_range_end":"[^"]+") "time_range_end":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_explain
ORDER BY vec_l2sq_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 2;

DROP TABLE vectors_explain;

-- ============================================
-- Part 2: Join with vector order/limit
-- ============================================
CREATE TABLE vectors_explain_left (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'l2sq'),
    PRIMARY KEY (vec_id)
);

CREATE TABLE vectors_explain_right (
    vec_id INT,
    note STRING,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_explain_left VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.9, 0.0]'),
    (3, '2024-01-01 00:00:02', '[0.0, 1.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, 0.9]');

INSERT INTO vectors_explain_right VALUES
    (3, 'keep', '2024-01-01 00:00:02'),
    (4, 'keep', '2024-01-01 00:00:03');

ADMIN FLUSH_TABLE('vectors_explain_left');

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE ("file_id":"[^"]+") "file_id":"REDACTED"
-- SQLNESS REPLACE ("time_range_start":"[^"]+") "time_range_start":"REDACTED"
-- SQLNESS REPLACE ("time_range_end":"[^"]+") "time_range_end":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT l.vec_id
FROM vectors_explain_left l
JOIN vectors_explain_right r ON l.vec_id = r.vec_id
ORDER BY vec_l2sq_distance(l.embedding, '[1.0, 0.0]'), l.vec_id
LIMIT 1;

DROP TABLE vectors_explain_left;
DROP TABLE vectors_explain_right;

-- ============================================
-- Part 3: Cosine and dot explain coverage
-- ============================================
CREATE TABLE vectors_explain_metric (
    vec_id INT,
    ts TIMESTAMP TIME INDEX,
    embedding VECTOR(2) NOT NULL VECTOR INDEX WITH (metric = 'cosine'),
    PRIMARY KEY (vec_id)
);

INSERT INTO vectors_explain_metric VALUES
    (1, '2024-01-01 00:00:00', '[1.0, 0.0]'),
    (2, '2024-01-01 00:00:01', '[0.0, 1.0]'),
    (3, '2024-01-01 00:00:02', '[-1.0, 0.0]'),
    (4, '2024-01-01 00:00:03', '[0.0, -1.0]');

ADMIN FLUSH_TABLE('vectors_explain_metric');

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE ("file_id":"[^"]+") "file_id":"REDACTED"
-- SQLNESS REPLACE ("time_range_start":"[^"]+") "time_range_start":"REDACTED"
-- SQLNESS REPLACE ("time_range_end":"[^"]+") "time_range_end":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_explain_metric
ORDER BY vec_cos_distance(embedding, '[1.0, 0.0]'), vec_id
LIMIT 2;

-- SQLNESS REPLACE ("metrics_per_partition":\s*.*metrics=) "metrics_per_partition": REDACTED metrics=
-- SQLNESS REPLACE (metrics=\{.*\}) metrics=REDACTED
-- SQLNESS REPLACE (metrics=\[[^\]]*\]) metrics=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE Hash\(\[vec_id@0\],.* Hash([vec_id@0],REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE ("file_id":"[^"]+") "file_id":"REDACTED"
-- SQLNESS REPLACE ("time_range_start":"[^"]+") "time_range_start":"REDACTED"
-- SQLNESS REPLACE ("time_range_end":"[^"]+") "time_range_end":"REDACTED"
-- SQLNESS REPLACE ("[a-z_]+":"[0-9\.]+(ns|us|µs|ms|s)") "DURATION": REDACTED
-- SQLNESS REPLACE ,\s*filter=.*?metrics=  metrics=
-- SQLNESS REPLACE Total\s+rows:\s+\d+ Total rows: REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE VERBOSE
SELECT vec_id
FROM vectors_explain_metric
ORDER BY vec_dot_product(embedding, '[1.0, 0.0]') DESC, vec_id
LIMIT 2;

DROP TABLE vectors_explain_metric;
