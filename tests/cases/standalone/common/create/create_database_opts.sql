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
       'compaction.type' = 'twcs',
       'compaction.twcs.time_window' = '1h',
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

--- test compaction options----

CREATE DATABASE test_compaction_opt;

USE test_compaction_opt;

SHOW CREATE DATABASE test_compaction_opt;

CREATE TABLE test_table(ts TIMESTAMP TIME INDEX, val INT);

SHOW CREATE TABLE test_table;

ALTER DATABASE test_compaction_opt SET 'compaction.type' = 'twcs';

ALTER DATABASE test_compaction_opt SET 'compaction.twcs.time_window' = '2h';

SHOW CREATE DATABASE test_compaction_opt;

SHOW CREATE TABLE test_table;

CREATE TABLE test_table2(ts TIMESTAMP TIME INDEX, val INT);

SHOW CREATE TABLE test_table2;

USE public;

DROP DATABASE test_compaction_opt;

CREATE DATABASE test_compaction_opt2;

USE test_compaction_opt2;

CREATE TABLE test_table(ts TIMESTAMP TIME INDEX, v INT) WITH ('compaction.type'='twcs','compaction.twcs.time_window'='1h');

SHOW CREATE TABLE test_table;

ALTER DATABASE test_compaction_opt2 SET 'compaction.twcs.time_window' = '3h';

SHOW CREATE TABLE test_table;

USE public;

DROP DATABASE test_compaction_opt2;

SHOW DATABASES;
