CREATE TABLE system_metrics (
  `id` INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT COMMENT 'comment',
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (`id`, host)
)
PARTITION ON COLUMNS (`id`) (
  `id` < 5,
  `id` >= 5 AND `id` < 9,
  `id` >= 9
)
ENGINE=mito
WITH(
  ttl = '7d',
  write_buffer_size = 1024
);

SHOW CREATE TABLE system_metrics;

SHOW CREATE TABLE system_metrics FOR POSTGRES_FOREIGN_TABLE;

DROP TABLE system_metrics;

create table table_without_partition (
  ts TIMESTAMP TIME INDEX NOT NULL DEFAULT current_timestamp()
);

show create table table_without_partition;

drop table table_without_partition;

CREATE TABLE not_supported_table_storage_option (
  `id` INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (`id`, host)
)
PARTITION ON COLUMNS (`id`) (
  `id` < 5,
  `id` >= 5 AND `id` < 9,
  `id` >= 9
)
ENGINE=mito
WITH(
  storage = 'S3'
);

CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

CREATE TABLE t1 (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

show create table phy;

show create table t1;

SHOW CREATE TABLE t1 FOR POSTGRES_FOREIGN_TABLE;

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

CREATE TABLE IF NOT EXISTS "phy" (
  "ts" TIMESTAMP(3) NOT NULL,
  "val" DOUBLE NULL,
  "host" STRING NULL SKIPPING INDEX WITH(granularity = '8192', type = 'BLOOM'),
  TIME INDEX ("ts"),
  PRIMARY KEY ("host"),
)
ENGINE=metric
WITH(
  'index.granularity' = '8192',
  'index.type' = 'skipping',
  physical_metric_table = ''
);

show create table phy;

CREATE TABLE t1 (
    ts TIMESTAMP TIME INDEX, 
    val DOUBLE, 
    job STRING PRIMARY KEY
) ENGINE=metric WITH (
    "on_physical_table" = "phy"
);

show index from phy;

drop table t1;

drop table phy;

show create table numbers;

show create table information_schema.columns;

CREATE TABLE "specify_invereted_index_cols" (
  "ts" TIMESTAMP(3) NOT NULL,
  "val" DOUBLE NULL,
  "host" STRING NULL,
  "job" STRING NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("host", "job"),
  INVERTED INDEX ("job")
);

show create table specify_invereted_index_cols;

drop table specify_invereted_index_cols;

CREATE TABLE "specify_empty_invereted_index_cols" (
  "ts" TIMESTAMP(3) NOT NULL,
  "val" DOUBLE NULL,
  "host" STRING NULL,
  "job" STRING NULL,
  TIME INDEX ("ts"),
  PRIMARY KEY ("host", "job"),
  INVERTED INDEX ()
);

show create table specify_empty_invereted_index_cols;

drop table specify_empty_invereted_index_cols;

CREATE TABLE test_table_constrain_composite_indexes (
  `id` INT,
  host STRING,
  ts TIMESTAMP,
  PRIMARY KEY (`id`, host),
  INVERTED INDEX (host),
  FULLTEXT INDEX (host) WITH (analyzer='English', case_sensitive='true'),
  SKIPPING INDEX (host, `id`),
  TIME INDEX (ts)
);

show create table test_table_constrain_composite_indexes;

drop table test_table_constrain_composite_indexes;

CREATE TABLE test_column_constrain_composite_indexes (
  `id` INT SKIPPING INDEX INVERTED INDEX,
  host STRING PRIMARY KEY SKIPPING INDEX FULLTEXT INDEX INVERTED INDEX,
  ts TIMESTAMP TIME INDEX,
);

show create table test_column_constrain_composite_indexes;

drop table test_column_constrain_composite_indexes;
