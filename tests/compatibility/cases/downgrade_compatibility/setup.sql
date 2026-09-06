CREATE TABLE t_downgrade_compatibility(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
);

INSERT INTO t_downgrade_compatibility VALUES
('2024-02-09 00:00:00+0000', 'host_a', 1),
('2024-02-09 00:01:00+0000', 'host_b', 2);

ADMIN FLUSH_TABLE('t_downgrade_compatibility');

CREATE TABLE t_preserve_sequence_downgrade(
    ts TIMESTAMP TIME INDEX,
    host STRING,
    val INT,
    PRIMARY KEY(host)
)
ENGINE=mito
WITH(append_mode='true', preserve_row_sequence='true');

INSERT INTO t_preserve_sequence_downgrade VALUES
('2024-02-09 00:00:00+0000', 'host_a', 1),
('2024-02-09 00:01:00+0000', 'host_b', 2);

ADMIN FLUSH_TABLE('t_preserve_sequence_downgrade');

INSERT INTO t_preserve_sequence_downgrade VALUES
('2024-02-09 00:02:00+0000', 'host_a', 3),
('2024-02-09 00:03:00+0000', 'host_c', 4);

ADMIN FLUSH_TABLE('t_preserve_sequence_downgrade');

ADMIN COMPACT_TABLE('t_preserve_sequence_downgrade');
