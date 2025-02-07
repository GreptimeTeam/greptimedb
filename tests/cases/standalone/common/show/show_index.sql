CREATE TABLE IF NOT EXISTS system_metrics (
    host STRING,
    idc STRING FULLTEXT INDEX,
    cpu_util DOUBLE,
    memory_util DOUBLE,
    disk_util DOUBLE,
    desc1 STRING,
    desc2 STRING FULLTEXT INDEX,
    desc3 STRING FULLTEXT INDEX,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(host, idc),
    INVERTED INDEX(idc, desc1, desc2),
    TIME INDEX(ts)
);

CREATE TABLE IF NOT EXISTS test (
    a STRING,
    b STRING SKIPPING INDEX,
    c DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(a, b),
    TIME INDEX(ts)
);

CREATE TABLE IF NOT EXISTS test_no_inverted_index (
    a STRING,
    b STRING SKIPPING INDEX,
    c DOUBLE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(a, b),
    TIME INDEX(ts)
);

show create table test_no_inverted_index;

SHOW INDEX;

SHOW INDEX FROM test;

SHOW INDEX FROM test_no_inverted_index;

SHOW INDEX FROM system_metrics;

SHOW INDEX FROM system_metrics in public;

SHOW INDEX FROM system_metrics like '%util%';

SHOW INDEX FROM system_metrics WHERE Key_name = 'TIME INDEX';

DROP TABLE system_metrics;

DROP TABLE test;

DROP TABLE test_no_inverted_index;
