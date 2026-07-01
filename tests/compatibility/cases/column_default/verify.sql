SELECT ts, host, val, msg, score FROM t_column_default ORDER BY ts, host;

INSERT INTO t_column_default (ts, host) VALUES
('2024-02-03 00:03:00+0000', 'host_d');

SELECT ts, host, val, msg, score FROM t_column_default WHERE host = 'host_d';

SHOW CREATE TABLE t_column_default;
