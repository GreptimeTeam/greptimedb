
-- SQLNESS ARG version=v0.9.5
CREATE TABLE test_ttl_0s(ts TIMESTAMP TIME INDEX, val INT) WITH (ttl = '0 second');

CREATE TABLE test_ttl_1s(ts TIMESTAMP TIME INDEX, val INT) WITH (ttl = '1 second');

CREATE TABLE test_ttl_none(ts TIMESTAMP TIME INDEX, val INT);

CREATE DATABASE ttl_db_1s WITH (ttl = '1 second');

CREATE DATABASE ttl_db_0s WITH (ttl = '0 second');

CREATE DATABASE ttl_db_none;

-- SQLNESS ARG version=latest
SHOW TABLES;

SHOW CREATE TABLE test_ttl_1s;

SHOW CREATE TABLE test_ttl_0s;

SHOW CREATE TABLE test_ttl_none;

DROP TABLE test_ttl_1s;

DROP TABLE test_ttl_0s;

DROP TABLE test_ttl_none;

SHOW DATABASES;

SHOW CREATE DATABASE ttl_db_1s;

SHOW CREATE DATABASE ttl_db_0s;

SHOW CREATE DATABASE ttl_db_none;

DROP DATABASE ttl_db_1s;

DROP DATABASE ttl_db_0s;

DROP DATABASE ttl_db_none;
