-- Set-comparison subqueries (`ANY`/`ALL`) must be rewritten before
-- PushDownFilter. Otherwise the set-comparison subquery can be pushed into
-- TableScan.partial_filters, which is not a valid remote scan filter.

CREATE TABLE sc_t (
    ts TIMESTAMP(3) TIME INDEX,
    v INT,
    PRIMARY KEY (v)
) ENGINE = mito;

CREATE TABLE sc_s (
    ts TIMESTAMP(3) TIME INDEX,
    v INT
) ENGINE = mito;

INSERT INTO sc_t VALUES
    ('2024-01-30 00:00:00', 1),
    ('2024-01-30 01:00:00', 6),
    ('2024-01-30 02:00:00', 10);

INSERT INTO sc_s VALUES
    ('2024-01-30 00:00:00', 5),
    ('2024-01-30 01:00:00', NULL);

ADMIN FLUSH_TABLE('sc_t');
ADMIN FLUSH_TABLE('sc_s');

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

SELECT v FROM sc_t WHERE v > ANY(SELECT v FROM sc_s) ORDER BY v;

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT v FROM sc_t WHERE v != ALL(SELECT v FROM sc_s) ORDER BY v;

SELECT v FROM sc_t WHERE v != ALL(SELECT v FROM sc_s) ORDER BY v;

DROP TABLE sc_t;
DROP TABLE sc_s;
