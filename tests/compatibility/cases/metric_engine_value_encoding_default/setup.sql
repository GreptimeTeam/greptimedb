CREATE TABLE metric_value_encoding_default_phy (
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE
) ENGINE=metric WITH ("physical_metric_table" = "");

CREATE TABLE metric_value_encoding_default (
    host STRING,
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE,
    PRIMARY KEY(host)
) ENGINE=metric WITH ("on_physical_table" = "metric_value_encoding_default_phy");

INSERT INTO metric_value_encoding_default (host, ts, greptime_value) VALUES
    ('host_a', '2024-01-01 00:00:00+0000', 1.5),
    ('host_b', '2024-01-01 00:01:00+0000', 2.5);

ADMIN FLUSH_TABLE('metric_value_encoding_default');
