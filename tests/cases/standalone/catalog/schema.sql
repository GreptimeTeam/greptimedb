CREATE SCHEMA test;

CREATE TABLE test.hello(i BIGINT TIME INDEX);

DROP TABLE test.hello;

DROP SCHEMA test;

CREATE SCHEMA test;

CREATE TABLE test.hello(i BIGINT TIME INDEX);

INSERT INTO test.hello VALUES (2), (3), (4);

SELECT * FROM test.hello;

DROP TABLE test.hello;

DROP SCHEMA test;

SELECT * FROM test.hello;
