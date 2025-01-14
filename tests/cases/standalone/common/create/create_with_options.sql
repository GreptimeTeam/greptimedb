CREATE TABLE not_supported_table_options_keys (
  `id` INT UNSIGNED,
  host STRING,
  cpu DOUBLE,
  disk FLOAT,
  ts TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  TIME INDEX (ts),
  PRIMARY KEY (id, host)
)
PARTITION ON COLUMNS (`id`) (
  `id` < 5,
  `id` >= 5 AND `id` < 9,
  `id` >= 9
)
ENGINE=mito
WITH(
  foo = 123,
  ttl = '7d',
  write_buffer_size = 1024
);

create table if not exists test_opts(
    host string,
    ts timestamp,
    cpu double default 0,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with(ttl='7d', 'compaction.type'='twcs', 'compaction.twcs.time_window'='1d');

drop table test_opts;

create table if not exists test_opts(
    host string,
    ts timestamp,
    cpu double default 0,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('ttl'='7d', 'compaction.type'='twcs', 'compaction.twcs.time_window'='1d');

drop table test_opts;

create table if not exists test_mito_options(
    host string,
    ts timestamp,
    cpu double default 0,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with(
    'ttl'='7d',
    'compaction.type'='twcs',
    'compaction.twcs.max_active_window_runs'='2',
    'compaction.twcs.max_inactive_window_runs'='2',
    'compaction.twcs.time_window'='1d',
    'index.inverted_index.ignore_column_ids'='1,2,3',
    'index.inverted_index.segment_row_count'='512',
    'wal_options'='{"wal.provider":"raft_engine"}',
    'memtable.type' = 'partition_tree',
);

drop table test_mito_options;

create table if not exists invalid_compaction(
    host string,
    ts timestamp,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('compaction.type'='twcs', 'compaction.twcs.max_active_window_runs'='8d');
