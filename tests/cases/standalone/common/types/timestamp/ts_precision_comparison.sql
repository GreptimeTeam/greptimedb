-- description: Regression test for issue #8214: TIMESTAMP predicate
-- incorrectly matches rows with higher fractional seconds.

CREATE TABLE ts_precision_bug (
    ts TIMESTAMP(3) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
);

INSERT INTO ts_precision_bug VALUES
    ('2026-06-02 03:49:59.999', 1),
    ('2026-06-02 03:50:00.000', 2),
    ('2026-06-02 03:50:00.195', 3),
    ('2026-06-02 03:50:01.000', 4);

SELECT ts, v FROM ts_precision_bug ORDER BY ts;

-- Core regression: <= with literal that has no fractional seconds.
SELECT ts, v FROM ts_precision_bug WHERE ts <= '2026-06-02 03:50:00' ORDER BY ts;

-- With ORDER BY ts DESC LIMIT 1
SELECT ts, v FROM ts_precision_bug WHERE ts <= '2026-06-02 03:50:00' ORDER BY ts DESC LIMIT 1;

-- >= with literal that has fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts >= '2026-06-02 03:50:00.195' ORDER BY ts;

-- >= with literal that has no fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts >= '2026-06-02 03:50:00' ORDER BY ts;

-- >= with higher precision literal
SELECT ts, v FROM ts_precision_bug WHERE ts >= '2026-06-02 03:50:00.195' ORDER BY ts ASC LIMIT 1;

-- < with literal that has no fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts < '2026-06-02 03:50:00' ORDER BY ts;

-- < with literal that has fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts < '2026-06-02 03:50:00.195' ORDER BY ts;

-- > with literal that has no fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts > '2026-06-02 03:50:00' ORDER BY ts;

-- > with literal that has fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts > '2026-06-02 03:50:00.195' ORDER BY ts;

-- = with exact match (fractional seconds)
SELECT ts, v FROM ts_precision_bug WHERE ts = '2026-06-02 03:50:00.195';

-- = with exact match (no fractional seconds)
SELECT ts, v FROM ts_precision_bug WHERE ts = '2026-06-02 03:50:00.000';

-- = with no fractional seconds on row that has fractional part
SELECT ts, v FROM ts_precision_bug WHERE ts = '2026-06-02 03:50:00';

-- <> with fractional seconds
SELECT ts, v FROM ts_precision_bug WHERE ts <> '2026-06-02 03:50:00.195' ORDER BY ts;

-- BETWEEN with mixed-precision boundaries
SELECT ts, v FROM ts_precision_bug
WHERE ts BETWEEN '2026-06-02 03:50:00' AND '2026-06-02 03:50:00.195'
ORDER BY ts;

-- Sub-second boundary: row exactly at boundary should match <=
SELECT ts, v FROM ts_precision_bug WHERE ts <= '2026-06-02 03:49:59.999' ORDER BY ts;

-- Test with TIMESTAMP(6) (microsecond precision)
CREATE TABLE ts_us_bug (
    ts TIMESTAMP(6) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
);

INSERT INTO ts_us_bug VALUES
    ('2026-06-02 03:49:59.999999', 1),
    ('2026-06-02 03:50:00.000000', 2),
    ('2026-06-02 03:50:00.000195', 3),
    ('2026-06-02 03:50:01.000000', 4);

-- <= with no fractional seconds, microsecond precision
SELECT ts, v FROM ts_us_bug WHERE ts <= '2026-06-02 03:50:00' ORDER BY ts;

-- >= with no fractional seconds, microsecond precision
SELECT ts, v FROM ts_us_bug WHERE ts >= '2026-06-02 03:50:00' ORDER BY ts;

-- = with exact microsecond value
SELECT ts, v FROM ts_us_bug WHERE ts = '2026-06-02 03:50:00.000195';

-- > with pure seconds literal
SELECT ts, v FROM ts_us_bug WHERE ts > '2026-06-02 03:50:00' ORDER BY ts;

-- Test with TIMESTAMP(9) (nanosecond precision)
CREATE TABLE ts_ns_bug (
    ts TIMESTAMP(9) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
);

INSERT INTO ts_ns_bug VALUES
    ('2026-06-02 03:49:59.999999999', 1),
    ('2026-06-02 03:50:00.000000000', 2),
    ('2026-06-02 03:50:00.000000195', 3),
    ('2026-06-02 03:50:01.000000000', 4);

-- <= with no fractional seconds, nanosecond precision
SELECT ts, v FROM ts_ns_bug WHERE ts <= '2026-06-02 03:50:00' ORDER BY ts;

-- >= with no fractional seconds, nanosecond precision
SELECT ts, v FROM ts_ns_bug WHERE ts >= '2026-06-02 03:50:00' ORDER BY ts;

-- = with exact nanosecond value
SELECT ts, v FROM ts_ns_bug WHERE ts = '2026-06-02 03:50:00.000000195';

-- > with pure seconds literal
SELECT ts, v FROM ts_ns_bug WHERE ts > '2026-06-02 03:50:00' ORDER BY ts;

-- Test with TIMESTAMP(0) (second precision - no fractional seconds at all)
CREATE TABLE ts_s_bug (
    ts TIMESTAMP(0) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
);

INSERT INTO ts_s_bug VALUES
    ('2026-06-02 03:49:59', 1),
    ('2026-06-02 03:50:00', 2),
    ('2026-06-02 03:50:01', 3);

-- <= with second precision
SELECT ts, v FROM ts_s_bug WHERE ts <= '2026-06-02 03:50:00' ORDER BY ts;

-- >= with second precision
SELECT ts, v FROM ts_s_bug WHERE ts >= '2026-06-02 03:50:00' ORDER BY ts;

-- < and > with second precision
SELECT ts, v FROM ts_s_bug WHERE ts < '2026-06-02 03:50:00' ORDER BY ts;

SELECT ts, v FROM ts_s_bug WHERE ts > '2026-06-02 03:50:00' ORDER BY ts;

-- Cross-precision comparison: TIMESTAMP(3) column, TIMESTAMP(9) literal
SELECT ts, v FROM ts_precision_bug WHERE ts <= '2026-06-02 03:50:00.000000195' ORDER BY ts;

SELECT ts, v FROM ts_precision_bug WHERE ts >= '2026-06-02 03:50:00.000000195' ORDER BY ts;

-- = with cross-precision
SELECT ts, v FROM ts_precision_bug WHERE ts = '2026-06-02 03:50:00.000';

-- Clean up
DROP TABLE ts_precision_bug;
DROP TABLE ts_us_bug;
DROP TABLE ts_ns_bug;
DROP TABLE ts_s_bug;
