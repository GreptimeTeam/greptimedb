SELECT ts, host, val FROM t_table_ttl ORDER BY ts, host;

SHOW CREATE TABLE t_table_ttl;

ALTER TABLE t_table_ttl SET 'ttl' = '30d';

SHOW CREATE TABLE t_table_ttl;

INSERT INTO t_table_ttl VALUES
('2099-02-08 00:02:00+0000', 'host_c', 3);

SELECT ts, host, val FROM t_table_ttl ORDER BY ts, host;
