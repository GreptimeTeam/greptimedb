CREATE TABLE test (a TIMESTAMP TIME INDEX, b INTEGER);

INSERT INTO test VALUES (11, 22), (12, 21), (13, 22);

SELECT a FROM test LIMIT 1;

SELECT a FROM test LIMIT 1.25;

SELECT a FROM test LIMIT 2-1;

SELECT a FROM test LIMIT a;

SELECT a FROM test LIMIT a+1;

SELECT a FROM test LIMIT SUM(42);

SELECT a FROM test LIMIT row_number() OVER ();

CREATE TABLE test2 (a STRING, ts TIMESTAMP TIME INDEX);

INSERT INTO test2 VALUES ('Hello World', 1);

SELECT * FROM test2 LIMIT 3;

select 1 limit date '1992-01-01';

CREATE TABLE integers(i TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT * FROM integers LIMIT 3;

SELECT * FROM integers LIMIT 4;

SELECT * FROM integers as int LIMIT (SELECT MIN(integers.i) FROM integers);


SELECT * FROM integers as int OFFSET (SELECT MIN(integers.i) FROM integers);

SELECT * FROM integers as int LIMIT (SELECT MAX(integers.i) FROM integers) OFFSET (SELECT MIN(integers.i) FROM integers);

SELECT * FROM integers as int LIMIT (SELECT max(integers.i) FROM integers where i > 5);

SELECT * FROM integers as int LIMIT (SELECT max(integers.i) FROM integers where i > 5);

SELECT * FROM integers as int LIMIT (SELECT NULL);

SELECT * FROM integers as int LIMIT (SELECT -1);

SELECT * FROM integers as int LIMIT (SELECT 'ab');

DROP TABLE integers;

DROP TABLE test;

DROP TABLE test2;
