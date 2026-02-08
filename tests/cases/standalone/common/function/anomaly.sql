-- Test anomaly detection window functions: anomaly_score_zscore, anomaly_score_mad, anomaly_score_iqr

-- Setup: main test table
CREATE TABLE anomaly_test(
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (host)
);

-- host-a: normal values ~50 with one outlier (200.0), then back to normal
INSERT INTO anomaly_test VALUES
    ('host-a', 48.0,  '2025-01-01 00:00:00'),
    ('host-a', 49.0,  '2025-01-01 00:01:00'),
    ('host-a', 50.0,  '2025-01-01 00:02:00'),
    ('host-a', 51.0,  '2025-01-01 00:03:00'),
    ('host-a', 52.0,  '2025-01-01 00:04:00'),
    ('host-a', 200.0, '2025-01-01 00:05:00'),
    ('host-a', 50.0,  '2025-01-01 00:06:00');

-- host-b: constant values (degenerate case: stddev=0, MAD=0, IQR=0)
INSERT INTO anomaly_test VALUES
    ('host-b', 10.0, '2025-01-01 00:00:00'),
    ('host-b', 10.0, '2025-01-01 00:01:00'),
    ('host-b', 10.0, '2025-01-01 00:02:00'),
    ('host-b', 10.0, '2025-01-01 00:03:00'),
    ('host-b', 10.0, '2025-01-01 00:04:00');

-- 1. Basic expanding window with PARTITION BY (three functions)
-- Expect: first 2 rows per partition -> NULL (< 3 samples), outlier row high score.
-- For host-b (constant series), zscore/mad/iqr are all 0.0.
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (PARTITION BY host ORDER BY ts), 2) AS zscore,
    ROUND(anomaly_score_mad(val) OVER (PARTITION BY host ORDER BY ts), 2) AS mad,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (PARTITION BY host ORDER BY ts), 2) AS iqr
FROM anomaly_test
ORDER BY host, ts;

-- 2. Time range window (RANGE INTERVAL '5 minutes' PRECEDING)
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (
        PARTITION BY host ORDER BY ts
        RANGE INTERVAL '5 minutes' PRECEDING
    ), 2) AS zscore_range,
    ROUND(anomaly_score_mad(val) OVER (
        PARTITION BY host ORDER BY ts
        RANGE INTERVAL '5 minutes' PRECEDING
    ), 2) AS mad_range,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (
        PARTITION BY host ORDER BY ts
        RANGE INTERVAL '5 minutes' PRECEDING
    ), 2) AS iqr_range
FROM anomaly_test
ORDER BY host, ts;

-- 3. Fixed sliding window (ROWS 4 PRECEDING)
-- After the outlier slides out of the window, scores should return to normal
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (
        PARTITION BY host ORDER BY ts
        ROWS 4 PRECEDING
    ), 2) AS zscore_rows
FROM anomaly_test
WHERE host = 'host-a'
ORDER BY ts;

-- 3b. Centered window (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
-- This previously caused array index out-of-bounds panic with BoundedWindowAggExec
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (
        PARTITION BY host ORDER BY ts
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ), 2) AS zscore_centered,
    ROUND(anomaly_score_mad(val) OVER (
        PARTITION BY host ORDER BY ts
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ), 2) AS mad_centered,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (
        PARTITION BY host ORDER BY ts
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ), 2) AS iqr_centered
FROM anomaly_test
WHERE host = 'host-a'
ORDER BY ts;

-- 3c. Leading window (ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)
-- Another non-causal frame; regression test for the bounded execution panic fix
SELECT
    host, ts, val,
    ROUND(anomaly_score_zscore(val) OVER (
        PARTITION BY host ORDER BY ts
        ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
    ), 2) AS zscore_leading
FROM anomaly_test
WHERE host = 'host-a'
ORDER BY ts;

-- 3d. Full unbounded window (UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING)
SELECT
    host, ts, val,
    ROUND(anomaly_score_zscore(val) OVER (
        PARTITION BY host ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ), 2) AS zscore_full
FROM anomaly_test
WHERE host = 'host-a'
ORDER BY ts;

-- 4. Different IQR k values: k=1.5 vs k=3.0
-- Larger k → wider fences → lower score
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (PARTITION BY host ORDER BY ts), 2) AS iqr_k15,
    ROUND(anomaly_score_iqr(val, 3.0) OVER (PARTITION BY host ORDER BY ts), 2) AS iqr_k30
FROM anomaly_test
WHERE host = 'host-a'
ORDER BY ts;

-- 5. Named window clause (WINDOW w AS ...)
-- Three functions sharing the same named window
SELECT
    host,
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER w, 2) AS zscore,
    ROUND(anomaly_score_mad(val) OVER w, 2) AS mad,
    ROUND(anomaly_score_iqr(val, 1.5) OVER w, 2) AS iqr
FROM anomaly_test
WINDOW w AS (PARTITION BY host ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY host, ts;

-- 6. Subquery filtering anomalous rows (WHERE score > threshold)
-- MAD score for outlier is ~67, use threshold 3.0 to filter
-- MAD score for host-b constant rows is 0.0, so this query returns only host-a outlier.
SELECT * FROM (
    SELECT
        host,
        ts,
        val,
        ROUND(anomaly_score_mad(val) OVER (PARTITION BY host ORDER BY ts), 2) AS mad
    FROM anomaly_test
) WHERE mad > 3.0
ORDER BY host, ts;

-- 7. NULL handling
CREATE TABLE anomaly_null_test(
    host STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (host)
);

INSERT INTO anomaly_null_test VALUES
    ('n', 10.0, '2025-01-01 00:00:00'),
    ('n', 20.0, '2025-01-01 00:01:00'),
    ('n', NULL, '2025-01-01 00:02:00'),
    ('n', 30.0, '2025-01-01 00:03:00'),
    ('n', 15.0, '2025-01-01 00:04:00');

-- NULL input row should produce NULL output; NULL should not affect window statistics
SELECT
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (ORDER BY ts), 2) AS zscore,
    ROUND(anomaly_score_mad(val) OVER (ORDER BY ts), 2) AS mad,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (ORDER BY ts), 2) AS iqr
FROM anomaly_null_test
ORDER BY ts;

-- 8. Insufficient samples: ROWS 1 PRECEDING gives at most 2 valid points → all NULL
SELECT
    ts,
    val,
    anomaly_score_zscore(val) OVER (ORDER BY ts ROWS 1 PRECEDING) AS zscore_insuf,
    anomaly_score_mad(val) OVER (ORDER BY ts ROWS 1 PRECEDING) AS mad_insuf,
    anomaly_score_iqr(val, 1.5) OVER (ORDER BY ts ROWS 1 PRECEDING) AS iqr_insuf
FROM anomaly_null_test
ORDER BY ts;

-- 9. Zero-spread with deviation: +inf branch
-- host-b had 5 constant 10.0 values; inserting one deviating value triggers +inf
-- for MAD and IQR (whose spread metrics are robust to a single outlier),
-- while zscore sees non-zero stddev and returns a finite value.
INSERT INTO anomaly_test VALUES ('host-b', 11.0, '2025-01-01 00:05:00');

SELECT
    ts,
    val,
    ROUND(anomaly_score_zscore(val) OVER (ORDER BY ts), 2) AS zscore,
    ROUND(anomaly_score_mad(val) OVER (ORDER BY ts), 2) AS mad,
    ROUND(anomaly_score_iqr(val, 1.5) OVER (ORDER BY ts), 2) AS iqr
FROM anomaly_test
WHERE host = 'host-b'
ORDER BY ts;

-- Cleanup
DROP TABLE anomaly_test;

DROP TABLE anomaly_null_test;
