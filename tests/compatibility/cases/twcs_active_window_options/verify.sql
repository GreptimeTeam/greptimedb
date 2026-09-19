SELECT host, ts, cpu
FROM t_twcs_active_window_options
ORDER BY host, ts;

SHOW CREATE TABLE t_twcs_active_window_options;

ALTER TABLE t_twcs_active_window_options
SET 'compaction.twcs.active_window.trigger_file_num' = '4';

INSERT INTO t_twcs_active_window_options VALUES
  ('host3', '2026-08-01 00:02:00+0000', 3.0);

SELECT host, ts, cpu
FROM t_twcs_active_window_options
ORDER BY host, ts;
