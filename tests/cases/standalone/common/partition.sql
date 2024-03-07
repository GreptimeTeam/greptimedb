CREATE TABLE my_table (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

-- SQLNESS REPLACE (\d{13}) ID
SELECT table_catalog, table_schema, table_name, partition_name, partition_expression, greptime_partition_id from information_schema.partitions WHERE table_name = 'my_table' ORDER BY table_catalog, table_schema, table_name, partition_name;

-- SQLNESS REPLACE (\d{13}) REGION_ID
-- SQLNESS REPLACE (\d{1}) PEER_ID
SELECT region_id, peer_id, is_leader, status FROM information_schema.greptime_region_peers ORDER BY peer_id;

INSERT INTO my_table VALUES
    (100, 'a', 1),
    (200, 'b', 2),
    (1100, 'c', 3),
    (1200, 'd', 4),
    (2000, 'e', 5),
    (2100, 'f', 6),
    (2200, 'g', 7),
    (2400, 'h', 8);

SELECT * FROM my_table;

DELETE FROM my_table WHERE a < 150;

SELECT * FROM my_table;

DELETE FROM my_table WHERE a < 2200 AND a > 1500;

SELECT * FROM my_table;

DELETE FROM my_table WHERE a < 2500;

SELECT * FROM my_table;

DROP TABLE my_table;

CREATE TABLE my_table (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) ();

-- SQLNESS REPLACE (\d{13}) ID
SELECT table_catalog, table_schema, table_name, partition_name, partition_expression, greptime_partition_id from information_schema.partitions WHERE table_name = 'my_table' ORDER BY table_catalog, table_schema, table_name, partition_name;

-- SQLNESS REPLACE (\d{13}) REGION_ID
-- SQLNESS REPLACE (\d{1}) PEER_ID
SELECT region_id, peer_id, is_leader, status FROM information_schema.greptime_region_peers ORDER BY peer_id;

INSERT INTO my_table VALUES
    (100, 'a', 1),
    (200, 'b', 2),
    (1100, 'c', 3),
    (1200, 'd', 4),
    (2000, 'e', 5),
    (2100, 'f', 6),
    (2200, 'g', 7),
    (2400, 'h', 8);
    
SELECT * FROM my_table;

DROP TABLE my_table;
