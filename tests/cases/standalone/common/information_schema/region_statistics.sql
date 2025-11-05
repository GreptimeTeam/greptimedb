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
SELECT SUM(region_rows),SUM(written_bytes_since_open), SUM(memtable_size), SUM(sst_size), SUM(index_size)
       FROM INFORMATION_SCHEMA.REGION_STATISTICS WHERE table_id
       IN (SELECT TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'test' and table_schema = 'public');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
SELECT data_length, index_length, avg_row_length, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'test';

DROP TABLE test;
