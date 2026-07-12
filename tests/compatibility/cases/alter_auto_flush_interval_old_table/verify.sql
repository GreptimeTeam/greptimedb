SELECT host, ts, cpu
FROM t_alter_auto_flush_interval_old_table
ORDER BY host, ts;

SHOW CREATE TABLE t_alter_auto_flush_interval_old_table;

ALTER TABLE t_alter_auto_flush_interval_old_table SET 'auto_flush_interval' = '5m';

SHOW CREATE TABLE t_alter_auto_flush_interval_old_table;

INSERT INTO t_alter_auto_flush_interval_old_table VALUES
  ('host3', '2026-07-07 00:02:00+0000', 3.0);

SELECT host, ts, cpu
FROM t_alter_auto_flush_interval_old_table
ORDER BY host, ts;

ALTER TABLE t_alter_auto_flush_interval_old_table SET 'auto_flush_interval' = '10m';

SHOW CREATE TABLE t_alter_auto_flush_interval_old_table;

ALTER TABLE t_alter_auto_flush_interval_old_table SET 'auto_flush_interval' = NULL;

SHOW CREATE TABLE t_alter_auto_flush_interval_old_table;
