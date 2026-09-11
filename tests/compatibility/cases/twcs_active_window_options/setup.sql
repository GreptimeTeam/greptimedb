CREATE TABLE t_twcs_active_window_options (
  host STRING,
  ts TIMESTAMP TIME INDEX,
  cpu DOUBLE,
  PRIMARY KEY(host)
) ENGINE=mito
WITH(
  'compaction.type' = 'twcs',
  'compaction.twcs.trigger_file_num' = '3'
);

INSERT INTO t_twcs_active_window_options VALUES
  ('host1', '2026-08-01 00:00:00+0000', 1.0),
  ('host2', '2026-08-01 00:01:00+0000', 2.0);

ADMIN FLUSH_TABLE('t_twcs_active_window_options');
