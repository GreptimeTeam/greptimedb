-- Migrated from DuckDB test: test/sql/join/test_complex_join_expr.test
CREATE TABLE test (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (4, 1, 1000), (2, 2, 2000);

CREATE TABLE test2 (b INTEGER, c INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test2 VALUES (1, 2, 3000), (3, 0, 4000);

-- INNER JOIN with complex expression
SELECT * FROM test JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a;

-- LEFT JOIN with complex expression
SELECT * FROM test LEFT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a;

-- RIGHT JOIN with complex expression
SELECT * FROM test RIGHT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a NULLS FIRST;

-- FULL JOIN with complex expression
SELECT * FROM test FULL OUTER JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a NULLS FIRST;

-- Basic equi-join
SELECT * FROM test JOIN test2 ON test.b = test2.b ORDER BY test.a;

DROP TABLE test2;

DROP TABLE test;
