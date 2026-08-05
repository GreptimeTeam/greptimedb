CREATE TABLE t_downgrade_compatibility(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
);

INSERT INTO t_downgrade_compatibility VALUES
('2024-02-09 00:00:00+0000', 'host_a', 1),
('2024-02-09 00:01:00+0000', 'host_b', 2);

ADMIN FLUSH_TABLE('t_downgrade_compatibility');
