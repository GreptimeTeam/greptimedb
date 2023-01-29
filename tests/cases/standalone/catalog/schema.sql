CREATE SCHEMA test_schema;

SHOW DATABASES;

CREATE TABLE test_schema.hello(i BIGINT TIME INDEX);

DROP TABLE test_schema.hello;

DROP SCHEMA test_schema;

CREATE SCHEMA test_schema;

CREATE TABLE test_schema.hello(i BIGINT TIME INDEX);

INSERT INTO test_schema.hello VALUES (2), (3), (4);

SELECT * FROM test_schema.hello;

SHOW TABLES;

SHOW TABLES FROM test_schema;

DROP TABLE test_schema.hello;

DROP TABLE test_schema.hello;

SHOW TABLES FROM test_schema;

DROP SCHEMA test_schema;

SELECT * FROM test_schema.hello;
