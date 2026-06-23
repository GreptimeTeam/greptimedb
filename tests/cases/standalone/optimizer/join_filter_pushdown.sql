-- Regression test for issue #8338: static side-local predicates on a JOIN input
-- should reach the remote region TableScan/SeqScan filters before MergeScan wrapping.

CREATE TABLE parent (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    v DOUBLE,
    PRIMARY KEY (k)
) ENGINE = mito;

CREATE TABLE child (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    PRIMARY KEY (k)
) ENGINE = mito;

INSERT INTO parent VALUES
    ('2024-01-30 00:00:00', 'a', 1.0),
    ('2024-01-30 01:00:00', 'b', 2.0);

INSERT INTO child VALUES
    ('2024-01-30 00:00:00', 'a'),
    ('2024-01-31 00:00:00', 'c');

ADMIN FLUSH_TABLE('parent');
ADMIN FLUSH_TABLE('child');

-- Query A: single table with time index filter (baseline).
-- The scan should have partial_filters with the time condition.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE peers=\[\d+\(\d+,\s+\d+\),\s\] peers=[REDACTED]
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
EXPLAIN SELECT * FROM parent WHERE ts >= '2024-01-30 00:00:00';

-- Query B: flat LEFT JOIN with WHERE on parent's ts.
-- The filter should be pushed into the left MergeScan remote_input,
-- appearing as partial_filters=[...] in the parent TableScan.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE peers=\[\d+\(\d+,\s+\d+\),\s\] peers=[REDACTED]
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
EXPLAIN SELECT * FROM parent p LEFT JOIN child c ON p.k = c.k WHERE p.ts >= '2024-01-30 00:00:00';

-- Query C: subquery pre-filter (workaround from #8338).
-- Should produce the same logical scan filter shape as query B.
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE peers=\[\d+\(\d+,\s+\d+\),\s\] peers=[REDACTED]
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
EXPLAIN SELECT * FROM (SELECT * FROM parent WHERE ts >= '2024-01-30 00:00:00') p LEFT JOIN child c ON p.k = c.k;

DROP TABLE parent;
DROP TABLE child;
