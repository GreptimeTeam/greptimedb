USE public;

CREATE TABLE test (
    a int primary key,
    b string,
    ts timestamp time index,
) PARTITION ON COLUMNS (a) (
    a < 10,
    a >= 10 AND a < 20,
    a >= 20,
);


INSERT INTO test VALUES
       (1, 'a', 1),
       (11, 'b', 11),
       (21, 'c', 21);

-- SQLNESS SLEEP 3s
-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- For regions using different WAL implementations, the manifest size may vary.
-- The remote WAL implementation additionally stores a flushed entry ID when creating the manifest.
SELECT SUM(region_rows), SUM(written_bytes_since_open), SUM(query_cpu_time_millis),
       SUM(query_scanned_bytes), SUM(memtable_size), SUM(sst_size), SUM(index_size)
       FROM INFORMATION_SCHEMA.REGION_STATISTICS WHERE table_id
       IN (SELECT TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'test' and table_schema = 'public');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
SELECT data_length, index_length, avg_row_length, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'test';

DROP TABLE test;

-- The projected columns are millisecond-typed. A nanosecond time index proves
-- min_timestamp floors and max_timestamp ceils, so the reported window stays a
-- superset of the data and cannot hide a region from a time-bounded lookup.
CREATE TABLE precise (
    a int primary key,
    ts timestamp(9) time index,
);

-- 1500000ns = 1.5ms floors to 1ms; 3999999ns = 3.999999ms ceils to 4ms.
INSERT INTO precise VALUES
       (1, 1500000),
       (2, 2500000),
       (3, 3999999);

-- SQLNESS SLEEP 3s
SELECT min_timestamp, max_timestamp
       FROM INFORMATION_SCHEMA.REGION_STATISTICS WHERE table_id
       IN (SELECT TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'precise' and table_schema = 'public');

DROP TABLE precise;

-- A region with no rows has nothing to bound, so both ends stay NULL rather
-- than collapsing onto the epoch.
CREATE TABLE empty_region (
    a int primary key,
    ts timestamp time index,
);

-- SQLNESS SLEEP 3s
SELECT min_timestamp, max_timestamp
       FROM INFORMATION_SCHEMA.REGION_STATISTICS WHERE table_id
       IN (SELECT TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'empty_region' and table_schema = 'public');

DROP TABLE empty_region;
