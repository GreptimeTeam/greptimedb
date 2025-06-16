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

INSERT INTO
    integers (host, i, ts)
VALUES
    ('550-A', 1, '2023-01-01 00:00:00'),
    ('550-B', 5, '2023-01-01 00:00:00'),
    ('550-A', 2, '2023-01-01 01:00:00'),
    ('550-W', 3, '2023-01-01 02:00:00'),
    ('550-W', 4, '2023-01-01 03:00:00');

-- count
SELECT
    count(i)
FROM
    integers;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    count(i)
FROM
    integers;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE input_partitions=(\d+) input_partitions=REDACTED
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
    ts
ORDER BY
    ts,
    count(i);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts
ORDER BY
    ts,
    count(i);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE input_partitions=(\d+) input_partitions=REDACTED
EXPLAIN ANALYZE
SELECT
    ts,
    count(i)
FROM
    integers
GROUP BY
    ts
ORDER BY
    ts,
    count(i);

SELECT
    date_bin('1 hour' :: INTERVAL, ts) as time_window,
    count(i)
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window,
    count(i);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    date_bin('1 hour' :: INTERVAL, ts) as time_window,
    count(i)
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window,
    count(i);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE input_partitions=(\d+) input_partitions=REDACTED
EXPLAIN ANALYZE
SELECT
    date_bin('1 hour' :: INTERVAL, ts) as time_window,
    count(i)
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window,
    count(i);

DROP TABLE integers;