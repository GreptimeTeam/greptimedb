CREATE TABLE system_metrics (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT COMMENT 'comment',
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION ON COLUMNS (id) (
  id < 5,
  id >= 5 AND id < 9,
  id >= 9
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

CREATE TABLE not_supported_table_storage_option (
  id INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION ON COLUMNS (id) (
  id < 5,
  id >= 5 AND id < 9,
  id >= 9
)
ENGINE=mito
WITH(
  storage = 'S3'
);

CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

show create table phy;

show create table t1;

drop table t1;

drop table phy;

CREATE TABLE IF NOT EXISTS "phy" (
  "ts" TIMESTAMP(3) NOT NULL,
  "val" DOUBLE NULL,
  "__table_id" INT UNSIGNED NOT NULL,
  "__tsid" BIGINT UNSIGNED NOT NULL,
  "host" STRING NULL,
  "job" STRING NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("__table_id", "__tsid", "host", "job")
)
ENGINE=mito
WITH(
   physical_metric_table = '',
);

show create table phy;

drop table phy;

show create table numbers;

show create table information_schema.columns;
