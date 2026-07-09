CREATE TABLE t_alter_auto_flush_interval_old_table (
  host STRING,
  ts TIMESTAMP TIME INDEX,
  cpu DOUBLE,
  PRIMARY KEY(host)
) ENGINE=mito;

INSERT INTO t_alter_auto_flush_interval_old_table VALUES
  ('host1', '2026-07-07 00:00:00+0000', 1.0),
  ('host2', '2026-07-07 00:01:00+0000', 2.0);

ADMIN FLUSH_TABLE('t_alter_auto_flush_interval_old_table');
