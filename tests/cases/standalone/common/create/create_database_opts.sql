CREATE DATABASE mydb WITH (ttl = '1h');

SHOW DATABASES;

SHOW FULL DATABASES;

SHOW CREATE DATABASE mydb;

USE mydb;

CREATE TABLE test(host STRING, cpu DOUBLE, ts TIMESTAMP TIME INDEX);

SHOW CREATE TABLE test;

USE public;

DROP DATABASE mydb;

---test more options----
CREATE DATABASE mydb WITH (
       ttl = '1h',
       'memtable.type'='partition_tree',
       'append_mode'='false',
       'merge_mode'='last_non_null',
       'skip_wal'='true');

use mydb;

SHOW FULL DATABASES;

CREATE TABLE test1(host STRING, cpu DOUBLE, ts TIMESTAMP TIME INDEX);

SHOW CREATE TABLE test1;

CREATE TABLE test2(host STRING, cpu DOUBLE, ts TIMESTAMP TIME INDEX) WITH (
       'append_mode'='true',
       'merge_mode'='',
       'skip_wal'='false');

SHOW CREATE TABLE test2;

INSERT INTO test2 VALUES('host1', 1.0, '2023-10-01 00:00:00');

SELECT * FROM test2;

USE public;

DROP DATABASE mydb;


SHOW DATABASES;
