-- SQLNESS ARG version=v0.9.5
CREATE TABLE mito_system_metrics (
    host STRING,
    idc STRING,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(host, idc),
    TIME INDEX(ts)
)ENGINE=mito;

INSERT INTO mito_system_metrics
VALUES
    ("host1", "idc_a", 11.8, 10.3, 10.3, 1667446797450),
    ("host2", "idc_a", 80.0, 70.3, 90.0, 1667446797450),
    ("host1", "idc_b", 50.0, 66.7, 40.6, 1667446797450);

CREATE TABLE phy (ts timestamp time index, cpu_util double) engine=metric with ("physical_metric_table" = "");

CREATE TABLE system_metrics (
    host STRING,
    cpu_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(host),
    TIME INDEX(ts)
)ENGINE=metric with ("on_physical_table" = "phy");

INSERT INTO system_metrics (host, cpu_util, ts)
VALUES
    ('host1', 11.8, 1667446797450),
    ('host2', 80.0, 1667446797450),
    ('host1', 50.0, 1667446797450);

-- SQLNESS ARG version=latest
SHOW CREATE TABLE mito_system_metrics;

SHOW CREATE TABLE system_metrics;

INSERT INTO mito_system_metrics
VALUES
    ("host3", "idc_a", 90.0, 70.3, 90.0, 1667446797450),
    ("host4", "idc_a", 70.0, 70.3, 90.0, 1667446797450),
    ("host5", "idc_a", 60.0, 70.3, 90.0, 1667446797450);

INSERT INTO system_metrics (host, cpu_util, ts)
VALUES
    ('host3', 90.0, 1667446797450),
    ('host4', 70.0, 1667446797450),
    ('host5', 60.0, 1667446797450);

SELECT * FROM mito_system_metrics;

SELECT * FROM system_metrics;

DROP TABLE mito_system_metrics;

DROP TABLE system_metrics;

DROP TABLE phy;
