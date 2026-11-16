-- A filter on `date_bin(stride, ts)` bounds the time index, so it prunes SST files.
-- This is what a filter on a view's `date_bin(...) AS time` column becomes once it is
-- pushed below the view's aggregate.
CREATE TABLE
    IF NOT EXISTS `power` (
        `topic` STRING NULL,
        `p` DOUBLE NULL,
        `ts` TIMESTAMP(9) NOT NULL,
        TIME INDEX (`ts`),
    ) ENGINE = mito
WITH
    (append_mode = 'true');

-- One SST file per day.
INSERT INTO
    power
VALUES
    ("a", 1, "2023-06-10 10:00:10"),
    ("a", 3, "2023-06-10 10:00:50"),
    ("b", 5, "2023-06-10 23:59:59");

ADMIN FLUSH_TABLE ('power');

INSERT INTO
    power
VALUES
    ("a", 10, "2023-06-11 00:00:00"),
    ("a", 20, "2023-06-11 00:00:30"),
    ("b", 30, "2023-06-11 12:00:00");

ADMIN FLUSH_TABLE ('power');

INSERT INTO
    power
VALUES
    ("a", 100, "2023-06-12 00:00:00"),
    ("b", 200, "2023-06-12 08:30:00");

ADMIN FLUSH_TABLE ('power');

CREATE VIEW power_per_minute AS
SELECT
    date_bin('1 minute' :: INTERVAL, ts) AS time,
    topic,
    avg(p) AS p
FROM
    power
GROUP BY
    time,
    topic;

SELECT * FROM power_per_minute WHERE time >= '2023-06-11 00:00:00' ORDER BY time, topic;

SELECT * FROM power_per_minute WHERE time > '2023-06-11 00:00:00' ORDER BY time, topic;

-- The bucket starting at 2023-06-10 23:59:00 holds a row from the first file.
SELECT * FROM power_per_minute WHERE time < '2023-06-11 00:00:00' ORDER BY time, topic;

SELECT * FROM power_per_minute WHERE time <= '2023-06-10 23:59:00' ORDER BY time, topic;

SELECT * FROM power_per_minute WHERE time = '2023-06-11 00:00:00' ORDER BY time, topic;

SELECT * FROM power_per_minute WHERE time BETWEEN '2023-06-11 00:00:00' AND '2023-06-11 23:59:00' ORDER BY time, topic;

SELECT * FROM power_per_minute WHERE '2023-06-12 00:00:00' <= time ORDER BY time, topic;

-- Only the file of 2023-06-12 is scanned.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE \\?"(file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM power_per_minute WHERE time >= '2023-06-12 00:00:00';

-- The files of 2023-06-10 and 2023-06-11 are scanned: the upper bound is widened by one stride.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE \\?"(file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM power_per_minute WHERE time < '2023-06-11 00:00:00';

DROP VIEW power_per_minute;

DROP TABLE power;

-- On a coarser time index, `date_bin` truncates its nanosecond bucket start to the
-- column unit, so the returned bucket can start up to one unit before the true one.
-- Each row below sits in its own file, which the upper bound must not prune.
CREATE TABLE
    IF NOT EXISTS `power_ms` (
        `p` DOUBLE NULL,
        `ts` TIMESTAMP(3) NOT NULL,
        TIME INDEX (`ts`),
    ) ENGINE = mito
WITH
    (append_mode = 'true');

INSERT INTO power_ms VALUES (1, "2023-06-10 00:00:00.001");

ADMIN FLUSH_TABLE ('power_ms');

INSERT INTO power_ms VALUES (2, "2023-06-10 00:00:00.010");

ADMIN FLUSH_TABLE ('power_ms');

INSERT INTO power_ms VALUES (3, "2023-06-11 00:00:00");

ADMIN FLUSH_TABLE ('power_ms');

-- An origin half a millisecond off: the bucket of 00:00:00.001 truly starts at
-- 00:00:00.0005 and is returned as 00:00:00.000.
CREATE VIEW power_ms_origin AS
SELECT
    date_bin(
        '1 millisecond' :: INTERVAL,
        ts,
        '2023-06-10 00:00:00.0005' :: TIMESTAMP(9)
    ) AS time,
    p
FROM
    power_ms;

SELECT * FROM power_ms_origin WHERE time = '2023-06-10 00:00:00' ORDER BY time;

SELECT * FROM power_ms_origin WHERE time <= '2023-06-10 00:00:00' ORDER BY time;

SELECT * FROM power_ms_origin WHERE time BETWEEN '2023-06-09 00:00:00' AND '2023-06-10 00:00:00' ORDER BY time;

-- A stride of 2.6 ms from midnight: the bucket of 00:00:00.010 truly starts at
-- 00:00:00.0078 and is returned as 00:00:00.007.
CREATE VIEW power_ms_stride AS
SELECT
    date_bin(
        '2600 microseconds' :: INTERVAL,
        ts,
        '2023-06-10 00:00:00' :: TIMESTAMP(9)
    ) AS time,
    p
FROM
    power_ms;

SELECT * FROM power_ms_stride WHERE time = '2023-06-10 00:00:00.007' ORDER BY time;

SELECT * FROM power_ms_stride WHERE time <= '2023-06-10 00:00:00.007' ORDER BY time;

DROP VIEW power_ms_origin;

DROP VIEW power_ms_stride;

DROP TABLE power_ms;
