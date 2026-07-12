CREATE TABLE t_last_row_merge(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    memory DOUBLE,
    PRIMARY KEY(host)
)
ENGINE=mito
WITH('merge_mode'='last_row');

INSERT INTO t_last_row_merge VALUES
('host1', 0, 0, NULL),
('host2', 1, NULL, 1);

INSERT INTO t_last_row_merge VALUES
('host1', 0, NULL, 10),
('host2', 1, 11, NULL);

ADMIN FLUSH_TABLE('t_last_row_merge');
