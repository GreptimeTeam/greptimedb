--- test flush_region and compact_region ---

CREATE TABLE my_table (
  a INT PRIMARY KEY,
  b STRING,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) ();

INSERT INTO my_table VALUES
    (1, 'a', 1),
    (2, 'b', 2),
    (11, 'c', 3),
    (12, 'd', 4),
    (21, 'e', 5),
    (22, 'f', 5);

SELECT * FROM my_table;

-- SELECT flush_region(greptime_partition_id) from information_schema.partitions WHERE table_name = 'my_table'; --

-- SELECT compact_region(greptime_partition_id) from information_schema.partitions WHERE table_name = 'my_table'; --

SELECT * FROM my_table;

DROP TABLE my_table;
