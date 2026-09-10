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
    'compaction.twcs.trigger_file_num'='2',
    'compaction.twcs.time_window'='1d',
    'index.inverted_index.ignore_column_ids'='1,2,3',
    'index.inverted_index.segment_row_count'='512',
    'wal_options'='{"wal.provider":"raft_engine"}',
    'memtable.type' = 'bulk',
);

drop table test_mito_options;

create table if not exists test_window_compaction_options(
    host string,
    ts timestamp,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with(
    'compaction.type'='twcs',
    'compaction.twcs.active_window.trigger_file_num'='4',
    'compaction.twcs.active_window.l1_merge_trigger'='8',
    'compaction.twcs.inactive_window.trigger_file_num'='3',
    'compaction.twcs.inactive_window.l1_merge_trigger'='12'
);

show create table test_window_compaction_options;

drop table test_window_compaction_options;

create table if not exists test_compaction_override_without_type(
    host string,
    ts timestamp,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('compaction.override'='true');

drop table test_compaction_override_without_type;

create table if not exists invalid_compaction(
    host string,
    ts timestamp,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('compaction.type'='twcs', 'compaction.twcs.trigger_file_num'='8d');

create table conflicting_twcs_trigger_aliases(
    host string,
    ts timestamp,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with(
    'compaction.twcs.trigger_file_num'='4',
    'compaction.twcs.active_window.trigger_file_num'='8'
);

create database conflicting_twcs_trigger_aliases with(
    'compaction.twcs.trigger_file_num'='4',
    'compaction.twcs.active_window.trigger_file_num'='8'
);
