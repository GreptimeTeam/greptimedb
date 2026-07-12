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
    ('220-A', 2, '2023-01-01 00:00:00'),
    ('220-B', 3, '2023-01-01 00:00:00'),
    ('550-A', 1, '2023-01-01 00:00:00'),
    ('550-B', 5, '2023-01-01 00:00:00'),
    ('550-A', 2, '2023-01-01 01:00:00'),
    ('550-W', 3, '2023-01-01 02:00:00'),
    ('550-Z', 4, '2023-01-01 02:00:00'),
    ('550-W', 5, '2023-01-01 03:00:00'),
    ('550-Z', 6, '2023-01-01 03:00:00');

SELECT
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
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
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN ANALYZE
SELECT
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers;

SELECT
    avg(i)
FROM
    integers;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT
    avg(i)
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
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN ANALYZE SELECT
    avg(i)
FROM
    integers;

SELECT
    ts,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    ts
ORDER BY
    ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    ts,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    ts
ORDER BY
    ts;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN ANALYZE
SELECT
    ts,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    ts
ORDER BY
    ts;


SELECT
    date_bin('2s'::INTERVAL, ts) as time_window,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    date_bin('2s'::INTERVAL, ts) as time_window,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- might write to different partitions
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
EXPLAIN ANALYZE
SELECT
    date_bin('2s'::INTERVAL, ts) as time_window,
    count(i),
    sum(i),
    uddsketch_calc(0.5, uddsketch_state(128, 0.01, i)),
    hll_count(hll(i))
FROM
    integers
GROUP BY
    time_window
ORDER BY
    time_window;

DROP TABLE integers;
