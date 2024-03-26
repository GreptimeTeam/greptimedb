CREATE TABLE IF NOT EXISTS system_metrics (
    host STRING,
    idc STRING,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(host, idc),
    TIME INDEX(ts)
);

SHOW COLUMNS;

SHOW COLUMNS FROM system_metrics;

SHOW COLUMNS FROM system_metrics in public;

SHOW FULL COLUMNS FROM `system_metrics`;

SHOW COLUMNS FROM system_metrics like '%util%';

SHOW COLUMNS FROM system_metrics WHERE Field = 'cpu_util';

DROP TABLE system_metrics;
