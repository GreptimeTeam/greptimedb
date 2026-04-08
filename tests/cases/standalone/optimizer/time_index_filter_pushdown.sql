-- Corresponding to issue TBD
-- An append-only mito table without primary key, and partitioned by a field column.
CREATE TABLE
    IF NOT EXISTS `cpu` (
        `rack` STRING NULL,
        `os` STRING NULL,
        `usage_small` SMALLINT NULL,
        `usage_user` BIGINT NULL,
        `greptime_timestamp` TIMESTAMP(9) NOT NULL,
        TIME INDEX (`greptime_timestamp`),
    ) PARTITION ON COLUMNS (`rack`) (
        rack < '2',
        rack >= '2'
        AND rack < '4',
        rack >= '4'
        AND rack < '6',
        rack >= '6'
        AND rack < '8',
        rack >= '8'
    ) ENGINE = mito
WITH
    (append_mode = 'true', sst_format = 'flat');

INSERT INTO
    cpu
VALUES
    ("1", "linux", 10, 10, "2023-06-12 01:04:49"),
    ("1", "linux", 15, 15, "2023-06-12 01:04:50"),
    ("3", "windows", 25, 25, "2023-06-12 01:05:00"),
    ("5", "mac", 30, 30, "2023-06-12 01:03:00"),
    ("7", "linux", 45, 45, "2023-06-12 02:00:00");

ADMIN FLUSH_TABLE ('cpu');

INSERT INTO
    cpu
VALUES
    ("2", "linux", 20, 20, "2023-06-12 01:04:51"),
    ("2", "windows", 22, 22, "2023-06-12 01:06:00"),
    ("4", "mac", 12, 12, "2023-06-12 00:59:00"),
    ("6", "linux", 35, 35, "2023-06-12 01:04:55"),
    ("8", "windows", 50, 50, "2023-06-12 02:10:00");

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT
    rack
FROM
    cpu
WHERE
    usage_small IN (10, 20);

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT
    rack
FROM
    cpu
WHERE
    usage_small BETWEEN 10 AND 20;

CREATE TABLE
    IF NOT EXISTS `cpu_single` (
        `rack` STRING NULL,
        `usage_small` SMALLINT NULL,
        `greptime_timestamp` TIMESTAMP(9) NOT NULL,
        TIME INDEX (`greptime_timestamp`),
    ) ENGINE = mito
WITH
    (append_mode = 'true', sst_format = 'flat');

INSERT INTO
    cpu_single
VALUES
    ("1", 10, "2023-06-12 01:04:49"),
    ("2", 20, "2023-06-12 01:04:50"),
    ("3", 25, "2023-06-12 01:05:00");

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE "partition_count":\{(.*?)\} "partition_count":REDACTED
-- SQLNESS REPLACE "flat_format":\s\w+, "flat_format": REDACTED,
EXPLAIN ANALYZE VERBOSE SELECT
    rack
FROM
    cpu_single
WHERE
    10 <= usage_small;

drop table cpu_single;

-- SQLNESS SORT_RESULT 3 1
select
    count(*)
from
    public.cpu
where
    greptime_timestamp > '2023-06-12 01:05:00';

-- SQLNESS SORT_RESULT 3 1
select
    os,
    count(*)
from
    public.cpu
where
    greptime_timestamp > '2023-06-12 01:05:00'
group by
    os;

-- SQLNESS SORT_RESULT 3 1
select
    os,
    count(*)
from
    cpu
where
    greptime_timestamp > '2023-06-12 01:05:00'
group by
    os;

drop table cpu;
