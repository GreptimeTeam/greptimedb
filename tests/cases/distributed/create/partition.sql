CREATE TABLE my_table (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX,
)
PARTITION BY RANGE COLUMNS (a) (
  PARTITION p0 VALUES LESS THAN (10),
  PARTITION p1 VALUES LESS THAN (20),
  PARTITION p2 VALUES LESS THAN (MAXVALUE),
);

-- SQLNESS REPLACE (\d{13}) ID
SELECT table_catalog, table_schema, table_name, partition_name, partition_expression, greptime_partition_id from information_schema.partitions WHERE table_name = 'my_table' ORDER BY table_catalog, table_schema, table_name, partition_name;

-- SQLNESS REPLACE (\d{13}) REGION_ID
-- SQLNESS REPLACE (\d{1}) PEER_ID
SELECT region_id, peer_id, is_leader, status FROM information_schema.region_peers ORDER BY peer_id;

DROP TABLE my_table;
