CREATE TABLE t_column_default(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT DEFAULT 42,
    msg STRING DEFAULT 'hello',
    score DOUBLE DEFAULT 1.5
);

INSERT INTO t_column_default (ts, host) VALUES
('2024-02-03 00:00:00+0000', 'host_a'),
('2024-02-03 00:01:00+0000', 'host_b');

INSERT INTO t_column_default (ts, host, val, msg, score) VALUES
('2024-02-03 00:02:00+0000', 'host_c', 100, 'override', 9.5);

ADMIN FLUSH_TABLE('t_column_default');
