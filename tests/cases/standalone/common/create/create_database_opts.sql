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

CREATE TABLE test(host STRING, cpu DOUBLE, ts TIMESTAMP TIME INDEX);

SHOW CREATE TABLE test;

USE public;

DROP DATABASE mydb;


SHOW DATABASES;
