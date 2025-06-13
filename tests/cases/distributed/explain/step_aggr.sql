CREATE TABLE integers(
    host STRING,
    i BIGINT,
    ts TIMESTAMP TIME INDEX
) PARTITION ON COLUMNS (host) (
    host < '550-A',
    host >= '550-A'
    AND host < '550-W',
    host >= '550-W'
);

INSERT INTO integers (host, i, ts) VALUES
    ('550-A', 1, '2023-01-01 00:00:00'),
    ('550-A', 2, '2023-01-01 01:00:00'),
    ('550-W', 3, '2023-01-01 02:00:00'),
    ('550-W', 4, '2023-01-01 03:00:00');

-- count
EXPLAIN SELECT
    count(i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    count(i)
FROM
    integers
GROUP BY
    ts;

-- sum
EXPLAIN SELECT
    sum(i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    sum(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    sum(i)
FROM
    integers
GROUP BY
    ts;

-- min
EXPLAIN SELECT
    min(i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    min(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    min(i)
FROM
    integers
GROUP BY
    ts;

-- max
EXPLAIN SELECT
    max(i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    max(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    max(i)
FROM
    integers
GROUP BY
    ts;

-- uddsketch_state
EXPLAIN SELECT
    uddsketch_state(128, 0.01, i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    uddsketch_state(128, 0.01, i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    uddsketch_state(128, 0.01, i)
FROM
    integers
GROUP BY
    ts;

-- hll
EXPLAIN SELECT
    hll(i)
FROM
    integers;

EXPLAIN SELECT
    ts,
    hll(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN SELECT
    hll(i)
FROM
    integers
GROUP BY
    ts;

DROP TABLE integers;