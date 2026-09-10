-- Regression coverage for aggregate expressions with multiple MAX aggregates.
CREATE DATABASE max_abs_regression;

USE max_abs_regression;

CREATE TABLE t (
    ts TIMESTAMP TIME INDEX,
    v INT,
    PRIMARY KEY (v),
) PARTITION ON COLUMNS (v) (
    v < 0,
    v >= 0
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

-- The plan must retain Partial and Final aggregation without an incomplete filter.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN SELECT MAX(ABS(v)), MAX(ts) FROM t;

DROP TABLE t;

USE public;

DROP DATABASE max_abs_regression;
