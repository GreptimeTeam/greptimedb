CREATE DATABASE test_ttl_db WITH (ttl = '1 second');

USE test_ttl_db;

CREATE TABLE test_ttl(ts TIMESTAMP TIME INDEX, val INT);

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER DATABASE test_ttl_db SET ttl = '1 day';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER TABLE test_ttl SET 'ttl' = '6 hours';

SHOW CREATE TABLE test_ttl;

ALTER TABLE test_ttl SET 'ttl' = 'immediate';

SHOW CREATE TABLE test_ttl;

ALTER TABLE test_ttl SET 'ttl' = '0s';

SHOW CREATE TABLE test_ttl;

ALTER TABLE test_ttl SET 'ttl' = 'forever';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER TABLE test_ttl UNSET 'ttl';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER DATABASE test_ttl_db SET 'ttl' = 'forever';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER DATABASE test_ttl_db SET 'ttl' = '0s';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER DATABASE test_ttl_db SET 'ttl' = 'immediate';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER DATABASE test_ttl_db UNSET 'ttl';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

ALTER TABLE test_ttl UNSET 'ttl';

SHOW CREATE TABLE test_ttl;

SHOW CREATE DATABASE test_ttl_db;

DROP TABLE test_ttl;

USE public;

DROP DATABASE test_ttl_db;