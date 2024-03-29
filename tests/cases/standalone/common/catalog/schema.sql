CREATE SCHEMA test_public_schema;

CREATE SCHEMA test_public_schema;

CREATE SCHEMA IF NOT EXISTS test_public_schema;

SHOW DATABASES LIKE '%public%';

SHOW DATABASES WHERE Database = 'test_public_schema';

USE test_public_schema;

CREATE TABLE hello(i TIMESTAMP TIME INDEX);

DROP TABLE hello;

CREATE TABLE hello(i TIMESTAMP TIME INDEX);

SHOW TABLES FROM test_public_schema;

SHOW TABLES FROM public;

INSERT INTO hello VALUES (2), (3), (4);

SELECT * FROM hello;

SHOW TABLES;

SHOW FULL TABLES WHERE Table_type != 'VIEW';

DROP TABLE hello;

DROP TABLE hello;

SHOW TABLES FROM test_public_schema;

SHOW TABLES FROM public;

SHOW TABLES FROM public WHERE Tables = 'numbers';

DROP SCHEMA test_public_schema;

SELECT * FROM test_public_schema.hello;

USE public;
