CREATE TABLE system_metrics (
    host STRING,
    idc STRING,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(host, idc),
    TIME INDEX(ts)
);

INSERT INTO system_metrics
VALUES
    ("host1", "idc_a", 11.8, 10.3, 10.3, 1667446797450),
    ("host2", "idc_a", 80.1, 70.3, 90.0, 1667446797450),
    ("host1", "idc_b", 50.0, 66.7, 40.6, 1667446797450);

SELECT * FROM system_metrics;

SELECT count(*) FROM system_metrics;

SELECT avg(cpu_util) FROM system_metrics;

SELECT idc, avg(memory_util) FROM system_metrics GROUP BY idc ORDER BY idc;

DROP TABLE system_metrics;
