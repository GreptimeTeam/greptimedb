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

-- SQLNESS SLEEP 11s
-- FIXME(dennis): we need to wait the datanode reporting stats info to metasrv.
SELECT SUM(region_rows), SUM(disk_size), SUM(sst_size), SUM(index_size)
       FROM INFORMATION_SCHEMA.REGION_STATISTICS WHERE table_id
       IN (SELECT TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'test' and table_schema = 'public');

DROP TABLE test;
