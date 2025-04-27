CREATE TABLE IF NOT EXISTS `test_multi_pk_filter` ( `namespace` STRING NULL, `env` STRING NULL DEFAULT 'NULL', `flag` INT NULL, `total` BIGINT NULL, `greptime_timestamp` TIMESTAMP(9) NOT NULL, TIME INDEX (`greptime_timestamp`), PRIMARY KEY (`namespace`, `env`, `flag`) ) ENGINE=mito WITH( ttl = '1month 29days 13h 26m 24s' );

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5289, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 0, 421, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 356, NOW());

ADMIN FLUSH_TABLE('test_multi_pk_filter');

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 412, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'dev', 1, 298, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5289, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 5874, NOW());

ADMIN FLUSH_TABLE('test_multi_pk_filter');

INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'production', 1, 6132, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1287, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1432, NOW());
INSERT INTO test_multi_pk_filter
    (namespace, env, flag, total, greptime_timestamp)
    VALUES ('thermostat_v2', 'testing', 1, 1056, NOW());

SELECT greptime_timestamp,namespace,env,total FROM test_multi_pk_filter WHERE
    greptime_timestamp BETWEEN NOW() - '1hour'::interval AND NOW() AND flag = 1 AND namespace = 'thermostat_v2';

SELECT greptime_timestamp,namespace,env,total FROM test_multi_pk_filter WHERE
    greptime_timestamp BETWEEN NOW() - '1hour'::interval AND NOW() AND flag = 1 AND namespace = 'thermostat_v2' AND env='dev';
