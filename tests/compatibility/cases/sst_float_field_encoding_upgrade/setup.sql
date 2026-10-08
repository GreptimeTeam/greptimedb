CREATE TABLE t_sst_float_field_encoding_upgrade(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    f FLOAT,
    d DOUBLE
) ENGINE=mito;

INSERT INTO t_sst_float_field_encoding_upgrade VALUES
('2024-02-10 00:00:00+0000', 'host_a', 1.25, 10.5),
('2024-02-10 00:01:00+0000', 'host_b', -2.5, 20.25),
('2024-02-10 00:02:00+0000', 'host_c', NULL, NULL);

ADMIN FLUSH_TABLE('t_sst_float_field_encoding_upgrade');
