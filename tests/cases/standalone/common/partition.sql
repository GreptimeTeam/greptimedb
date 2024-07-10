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
SELECT region_id, peer_id, is_leader, status FROM information_schema.region_peers ORDER BY peer_id;

INSERT INTO my_table VALUES
    (100, 'a', 1),
    (200, 'b', 2),
    (1100, 'c', 3),
    (1200, 'd', 4),
    (2000, 'e', 5),
    (2100, 'f', 6),
    (2200, 'g', 7),
    (2400, 'h', 8);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM my_table;

DELETE FROM my_table WHERE a < 150;

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM my_table;

DELETE FROM my_table WHERE a < 2200 AND a > 1500;

-- SQLNESS SORT_RESULT 3 1
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
SELECT region_id, peer_id, is_leader, status FROM information_schema.region_peers ORDER BY peer_id;

INSERT INTO my_table VALUES
    (100, 'a', 1),
    (200, 'b', 2),
    (1100, 'c', 3),
    (1200, 'd', 4),
    (2000, 'e', 5),
    (2100, 'f', 6),
    (2200, 'g', 7),
    (2400, 'h', 8);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM my_table;

DROP TABLE my_table;

-- incorrect partition rule
CREATE TABLE invalid_rule (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 10,
  a > 10 AND a < 20,
  a >= 20
);

-- Issue https://github.com/GreptimeTeam/greptimedb/issues/4247
-- Partition rule with unary operator
CREATE TABLE `molestiAe` (
  `sImiLiQUE` FLOAT NOT NULL,
  `amEt` TIMESTAMP(6) TIME INDEX,
  `EXpLICaBo` DOUBLE,
  PRIMARY KEY (`sImiLiQUE`)
) PARTITION ON COLUMNS (`sImiLiQUE`) (
  `sImiLiQUE` < -1,
  `sImiLiQUE` >= -1 AND `sImiLiQUE` < -0,
  `sImiLiQUE` >= 0
);

INSERT INTO `molestiAe` VALUES
  (-2, 0, 0),
  (-0.9, 0, 0),
  (1, 0, 0);

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM `molestiAe`;

DROP TABLE `molestiAe`;
