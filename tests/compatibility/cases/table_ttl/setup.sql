CREATE TABLE t_table_ttl(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
)
WITH('ttl'='7d');

INSERT INTO t_table_ttl VALUES
('2099-02-08 00:00:00+0000', 'host_a', 1),
('2099-02-08 00:01:00+0000', 'host_b', 2);

ADMIN FLUSH_TABLE('t_table_ttl');
