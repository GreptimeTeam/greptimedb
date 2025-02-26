SELECT * FROM (SELECT SUM(number) FROM numbers LIMIT 100000000000) LIMIT 0;

EXPLAIN SELECT * FROM (SELECT SUM(number) FROM numbers LIMIT 100000000000) LIMIT 0;

EXPLAIN SELECT * FROM (SELECT SUM(number) FROM numbers LIMIT 100000000000) WHERE 1=0;

CREATE TABLE test (a TIMESTAMP TIME INDEX, b INTEGER);

INSERT INTO test VALUES (11, 23), (12, 21), (13, 22);

SELECT a FROM test LIMIT 1;

SELECT b FROM test ORDER BY b LIMIT 2 OFFSET 0;

DROP TABLE test;
