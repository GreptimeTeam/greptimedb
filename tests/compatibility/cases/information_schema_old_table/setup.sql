CREATE TABLE t_information_schema_old_table(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE,
    code INT
);

INSERT INTO t_information_schema_old_table VALUES
('2024-02-09 00:00:00+0000', 'host_a', 1.0, 200),
('2024-02-09 00:01:00+0000', 'host_b', 2.0, 500);

ADMIN FLUSH_TABLE('t_information_schema_old_table');
