CREATE TABLE t_analyze_verbose_remote_metrics_extension(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val INT
)
PARTITION ON COLUMNS (host) (
    host < 'm',
    host >= 'm'
);

INSERT INTO t_analyze_verbose_remote_metrics_extension VALUES
('2024-02-04 00:00:00+0000', 'host_a', 1),
('2024-02-04 00:01:00+0000', 'host_z', 2);

ADMIN FLUSH_TABLE('t_analyze_verbose_remote_metrics_extension');
