CREATE TABLE system_metrics (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  n INT COMMENT 'range key',
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION BY RANGE COLUMNS (n) (
    PARTITION r0 VALUES LESS THAN (5),
    PARTITION r1 VALUES LESS THAN (9),
    PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito;

SHOW CREATE TABLE system_metrics;

DROP TABLE system_metrics;

create table table_without_partition (
  ts TIMESTAMP TIME INDEX NOT NULL DEFAULT current_timestamp()
);

show create table table_without_partition;

drop table table_without_partition;
