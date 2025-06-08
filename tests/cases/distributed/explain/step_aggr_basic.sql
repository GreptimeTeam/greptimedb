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
    date_bin('1 hour'::INTERVAL, ts),
    count(i)
FROM
    integers
GROUP BY
    date_bin('1 hour'::INTERVAL, ts);

DROP TABLE integers;