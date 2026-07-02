CREATE TABLE t_prefiltered_read(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    site STRING,
    val INT,
    cpu_load DOUBLE,
    active BOOLEAN
);

INSERT INTO t_prefiltered_read VALUES
('2024-02-04 00:00:00+0000', 'host_a', 'us-east', 1, 0.10, true),
('2024-02-04 00:01:00+0000', 'host_b', 'us-east', 5, 0.50, false),
('2024-02-04 00:02:00+0000', 'host_c', 'eu-west', 8, 0.80, true),
('2024-02-04 00:03:00+0000', 'host_d', 'eu-west', 13, 1.30, false),
('2024-02-04 00:04:00+0000', 'host_e', 'ap-south', 21, 2.10, true);

ADMIN FLUSH_TABLE('t_prefiltered_read');
