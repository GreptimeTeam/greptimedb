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
-- SQLNESS REPLACE (Hash.*) REDACTED
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
-- SQLNESS REPLACE (Hash.*) REDACTED
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
-- SQLNESS REPLACE (Hash.*) REDACTED
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

SELECT
    ts::BIGINT + 1,
    i / 2,
    count(i)
FROM
    integers
GROUP BY
    ts::BIGINT + 1,
    i / 2,
ORDER BY
    ts::BIGINT + 1,
    i / 2,
;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    ts::BIGINT + 1,
    i / 2,
    count(i)
FROM
    integers
GROUP BY
    ts::BIGINT + 1,
    i / 2,
ORDER BY
    ts::BIGINT + 1,
    i / 2,
;

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
    ts::BIGINT + 1,
    i / 2,
    count(i)
FROM
    integers
GROUP BY
    ts::BIGINT + 1,
    i / 2,
ORDER BY
    ts::BIGINT + 1,
    i / 2,
;


-- test udd/hll_merege pushdown
CREATE TABLE sink_table (
    time_window TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    udd_state BINARY,
    hll_state BINARY,
) PARTITION ON COLUMNS (host) (
    host < '550-A',
    host >= '550-A'
    AND host < '550-W',
    host >= '550-W'
);

INSERT INTO
    sink_table
SELECT
    date_bin('1 hour' :: INTERVAL, ts) as time_window,
    host,
    uddsketch_state(128, 0.01, i) as udd_state,
    hll(i) as hll_state
FROM
    integers
GROUP BY
    time_window,
    host;

SELECT
    uddsketch_calc(0.5, uddsketch_merge(128, 0.01, udd_state)) as udd_result,
    hll_count(hll_merge(hll_state)) as hll_result
FROM
    sink_table;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT
    uddsketch_calc(0.5, uddsketch_merge(128, 0.01, udd_state)) as udd_result,
    hll_count(hll_merge(hll_state)) as hll_result
FROM
    sink_table;

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
    uddsketch_calc(0.5, uddsketch_merge(128, 0.01, udd_state)) as udd_result,
    hll_count(hll_merge(hll_state)) as hll_result
FROM
    sink_table;

DROP TABLE integers;

DROP TABLE sink_table;