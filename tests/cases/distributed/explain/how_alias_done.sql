CREATE TABLE IF NOT EXISTS base_table (
  "time" TIMESTAMP(3) NOT NULL,
  env STRING NULL,
  service_name STRING NULL,
  city STRING NULL,
  page STRING NULL,
  lcp BIGINT NULL,
  fmp BIGINT NULL,
  fcp BIGINT NULL,
  fp BIGINT NULL,
  tti BIGINT NULL,
  fid BIGINT NULL,
  shard_key BIGINT NULL,
  TIME INDEX ("time"),
  PRIMARY KEY (env, service_name)
) PARTITION ON COLUMNS (shard_key) (
  shard_key < 4,
  shard_key >= 4
  AND shard_key < 8,
  shard_key >= 8
  AND shard_key < 12,
  shard_key >= 12
  AND shard_key < 16,
  shard_key >= 16
  AND shard_key < 20,
  shard_key >= 20
  AND shard_key < 24,
  shard_key >= 24
  AND shard_key < 28,
  shard_key >= 28
  AND shard_key < 32,
  shard_key >= 32
  AND shard_key < 36,
  shard_key >= 36
  AND shard_key < 40,
  shard_key >= 40
  AND shard_key < 44,
  shard_key >= 44
  AND shard_key < 48,
  shard_key >= 48
  AND shard_key < 52,
  shard_key >= 52
  AND shard_key < 56,
  shard_key >= 56
  AND shard_key < 60,
  shard_key >= 60
) ENGINE = mito WITH(
  'append_mode' = 'true',
  'compaction.twcs.max_output_file_size' = "2GB",
  'compaction.twcs.time_window' = "1h",
  'compaction.type' = "twcs",
);

EXPLAIN SELECT count(*) from base_table where time >= now();

-- ERROR:  Internal error: 1003
EXPLAIN ANALYZE SELECT count(*) from base_table where time >= now();

DROP TABLE base_table;
