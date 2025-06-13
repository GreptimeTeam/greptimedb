CREATE TABLE integers(
    host STRING PRIMARY KEY,
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
    ('550-B', 7, '2023-01-01 00:00:00'),
    ('550-A', 2, '2023-01-01 01:00:00'),
    ('550-W', 3, '2023-01-01 02:00:00'),
    ('550-W', 4, '2023-01-01 03:00:00');


-- count
SELECT
    count(i)
FROM
    integers;

EXPLAIN
SELECT
    count(i)
FROM
    integers;

EXPLAIN ANALYZE
SELECT
    count(i)
FROM
    integers;

SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN
SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts;

EXPLAIN ANALYZE
SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts;

SELECT
    date_bin('1 hour' :: INTERVAL, ts),
    count(i)
FROM
    integers
GROUP BY
    date_bin('1 hour' :: INTERVAL, ts);

EXPLAIN
SELECT
    date_bin('1 hour' :: INTERVAL, ts),
    count(i)
FROM
    integers
GROUP BY
    date_bin('1 hour' :: INTERVAL, ts);

EXPLAIN ANALYZE
SELECT
    date_bin('1 hour' :: INTERVAL, ts),
    count(i)
FROM
    integers
GROUP BY
    date_bin('1 hour' :: INTERVAL, ts);

DROP TABLE integers;