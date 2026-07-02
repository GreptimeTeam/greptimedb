SELECT host, ts, cpu
FROM t_last_row_append_mode
ORDER BY host, ts, cpu;

ALTER TABLE t_last_row_append_mode SET 'append_mode' = 'true';

SHOW CREATE TABLE t_last_row_append_mode;

INSERT INTO t_last_row_append_mode VALUES
('host1', 0, 100.0),
('host2', 1, 200.0);

SELECT host, ts, cpu
FROM t_last_row_append_mode
ORDER BY host, ts, cpu;
