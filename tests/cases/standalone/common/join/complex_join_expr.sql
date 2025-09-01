-- Migrated from DuckDB test: test/sql/join/test_complex_join_expr.test
-- Description: Test joins with comparisons involving both sides of the join
-- Note: Adapted for GreptimeDB

-- Create test tables
CREATE TABLE test (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (4, 1, 1000), (2, 2, 2000);

CREATE TABLE test2 (b INTEGER, c INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test2 VALUES (1, 2, 3000), (3, 0, 4000);

-- Test 1: INNER JOIN with complex expression
SELECT * FROM test JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a;

-- Test 2: LEFT JOIN with complex expression
SELECT * FROM test LEFT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a;

-- Test 3: RIGHT JOIN with complex expression  
SELECT * FROM test RIGHT JOIN test2 ON test.a+test2.c=test.b+test2.b ORDER BY test.a NULLS FIRST;

-- Test 4: Basic equi-join for comparison
SELECT * FROM test JOIN test2 ON test.b = test2.b ORDER BY test.a;

-- Clean up
DROP TABLE test2;
DROP TABLE test;