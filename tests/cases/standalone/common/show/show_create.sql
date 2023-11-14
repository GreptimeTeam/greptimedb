CREATE TABLE system_metrics (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT COMMENT 'comment',
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION BY RANGE COLUMNS (id) (
    PARTITION r0 VALUES LESS THAN (5),
    PARTITION r1 VALUES LESS THAN (9),
    PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito
WITH(
  ttl = '7d',
  write_buffer_size = 1024
);

SHOW CREATE TABLE system_metrics;

DROP TABLE system_metrics;

create table table_without_partition (
  ts TIMESTAMP TIME INDEX NOT NULL DEFAULT current_timestamp()
);

show create table table_without_partition;

drop table table_without_partition;

CREATE TABLE not_supported_table_options_keys (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION BY RANGE COLUMNS (id) (
    PARTITION r0 VALUES LESS THAN (5),
    PARTITION r1 VALUES LESS THAN (9),
    PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito
WITH(
  foo = 123,
  ttl = '7d',
  write_buffer_size = 1024
);
CREATE TABLE not_supported_table_storage_option (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION BY RANGE COLUMNS (id) (
    PARTITION r0 VALUES LESS THAN (5),
    PARTITION r1 VALUES LESS THAN (9),
    PARTITION r2 VALUES LESS THAN (MAXVALUE),
)
ENGINE=mito
WITH(
  storage = 'S3'
);
