-- Scalar subquery predicates must be converted before PushDownFilter. Otherwise
-- a scalar subquery can be pushed into TableScan.partial_filters, which is not a
-- valid remote scan filter.

CREATE TABLE scalar_fact (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    val DOUBLE,
    PRIMARY KEY (k)
) ENGINE = mito;

CREATE TABLE scalar_dim (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    threshold DOUBLE,
    PRIMARY KEY (k)
) ENGINE = mito;

INSERT INTO scalar_fact VALUES
    ('2024-01-30 00:00:00', 'a', 10.0),
    ('2024-01-30 01:00:00', 'b', 20.0),
    ('2024-01-30 02:00:00', 'c', 30.0),
    ('2024-01-30 03:00:00', 'd', 40.0);

INSERT INTO scalar_dim VALUES
    ('2024-01-30 00:00:00', 'a', 5.0),
    ('2024-01-30 01:00:00', 'b', 25.0),
    ('2024-01-30 02:00:00', 'c', NULL);

ADMIN FLUSH_TABLE('scalar_fact');
ADMIN FLUSH_TABLE('scalar_dim');

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE Hash\(\[[^\]]+\],.* Hash([REDACTED
-- SQLNESS REPLACE input_partitions=\d+ input_partitions=REDACTED
EXPLAIN SELECT f.k, f.val FROM scalar_fact f
WHERE f.val > (
    SELECT max(d.threshold) FROM scalar_dim d WHERE d.k = f.k
)
ORDER BY f.k;

SELECT f.k, f.val FROM scalar_fact f
WHERE f.val > (
    SELECT max(d.threshold) FROM scalar_dim d WHERE d.k = f.k
)
ORDER BY f.k;

DROP TABLE scalar_fact;
DROP TABLE scalar_dim;
