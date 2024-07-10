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

CREATE TABLE IF NOT EXISTS test (
    a STRING,
    b STRING,
    c DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(a, b),
    TIME INDEX(ts)
);

SHOW INDEX;

SHOW INDEX FROM test;

SHOW INDEX FROM system_metrics;

SHOW INDEX FROM system_metrics in public;

SHOW INDEX FROM system_metrics like '%util%';

SHOW INDEX FROM system_metrics WHERE Key_name = 'TIME INDEX';

DROP TABLE system_metrics;

DROP TABLE test;
