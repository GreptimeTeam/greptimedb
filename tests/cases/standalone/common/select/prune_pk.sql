CREATE TABLE IF NOT EXISTS `test_multi_pk_filter` ( `namespace` STRING NULL, `env` STRING NULL DEFAULT 'NULL', `flag` INT NULL, `total` BIGINT NULL, `greptime_timestamp` TIMESTAMP(9) NOT NULL, TIME INDEX (`greptime_timestamp`), PRIMARY KEY (`namespace`, `env`, `flag`) ) ENGINE=mito;

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5289, '2023-05-15 10:00:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 0, 421, '2023-05-15 10:05:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 356, '2023-05-15 10:10:00');

ADMIN FLUSH_TABLE('test_multi_pk_filter');

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 412, '2023-05-15 10:15:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 298, '2023-05-15 10:20:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5289, '2023-05-15 10:25:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5874, '2023-05-15 10:30:00');

ADMIN FLUSH_TABLE('test_multi_pk_filter');

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 6132, '2023-05-15 10:35:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1287, '2023-05-15 10:40:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1432, '2023-05-15 10:45:00');
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1056, '2023-05-15 10:50:00');

SELECT greptime_timestamp, namespace, env, total FROM test_multi_pk_filter WHERE
    greptime_timestamp BETWEEN '2023-05-15 10:00:00' AND '2023-05-15 11:00:00' AND flag = 1 AND namespace = 'thermostat_v2'
    ORDER BY greptime_timestamp;

SELECT greptime_timestamp, namespace, env, total FROM test_multi_pk_filter WHERE
    greptime_timestamp BETWEEN '2023-05-15 10:00:00' AND '2023-05-15 11:00:00' AND flag = 1 AND namespace = 'thermostat_v2' AND env='dev'
    ORDER BY greptime_timestamp;

DROP TABLE test_multi_pk_filter;

CREATE TABLE IF NOT EXISTS `test_multi_pk_null` ( `namespace` STRING NULL, `env` STRING NULL DEFAULT 'NULL', `total` BIGINT NULL, `greptime_timestamp` TIMESTAMP(9) NOT NULL, TIME INDEX (`greptime_timestamp`), PRIMARY KEY (`namespace`, `env`) ) ENGINE=mito;

INSERT INTO test_multi_pk_null
    (namespace, env, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 5289, '2023-05-15 10:00:00');
INSERT INTO test_multi_pk_null
    (namespace, env, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 421, '2023-05-15 10:05:00');

ADMIN FLUSH_TABLE('test_multi_pk_null');

SELECT * FROM test_multi_pk_null WHERE env IS NOT NULL;

DROP TABLE test_multi_pk_null;
