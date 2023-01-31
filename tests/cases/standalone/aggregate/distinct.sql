CREATE TABLE test_distinct (a INTEGER, b INTEGER, t BIGINT TIME INDEX);

INSERT INTO test_distinct VALUES (11, 22, 1), (13, 22, 2), (11, 21, 3), (11, 22, 4);

SELECT DISTINCT a, b FROM test_distinct ORDER BY a, b;

SELECT DISTINCT test_distinct.a, b FROM test_distinct ORDER BY a, b;

SELECT DISTINCT a FROM test_distinct ORDER BY a;

SELECT DISTINCT b FROM test_distinct ORDER BY b;

SELECT DISTINCT a, SUM(B) FROM test_distinct GROUP BY a ORDER BY a;

SELECT DISTINCT MAX(b) FROM test_distinct GROUP BY a;

SELECT DISTINCT CASE WHEN a > 11 THEN 11 ELSE a END FROM test_distinct;

DROP TABLE test_distinct;
