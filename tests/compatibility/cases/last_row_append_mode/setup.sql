CREATE TABLE t_last_row_append_mode(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
)
ENGINE=mito
WITH('merge_mode'='last_row');

INSERT INTO t_last_row_append_mode VALUES
('host1', 0, 1.0),
('host2', 1, 2.0);

INSERT INTO t_last_row_append_mode VALUES
('host1', 0, 10.0),
('host2', 1, 20.0);

ADMIN FLUSH_TABLE('t_last_row_append_mode');
