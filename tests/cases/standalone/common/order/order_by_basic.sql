-- Migrated from DuckDB test: test/sql/order/test_order_by.test
-- Test ORDER BY keyword

CREATE TABLE test(a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (11, 22, 1000), (12, 21, 2000), (13, 22, 3000);

-- Simple ORDER BY
SELECT b FROM test ORDER BY a DESC;

SELECT a, b FROM test ORDER BY a;

SELECT a, b FROM test ORDER BY a DESC;

-- ORDER BY on multiple columns
SELECT a, b FROM test ORDER BY b, a;

-- ORDER BY using select indices
SELECT a, b FROM test ORDER BY 2, 1;

SELECT a, b FROM test ORDER BY b DESC, a;

SELECT a, b FROM test ORDER BY b, a DESC;

-- TOP N queries with LIMIT
SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1;

-- OFFSET
SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1 OFFSET 1;

-- OFFSET without limit
SELECT a, b FROM test ORDER BY b, a DESC OFFSET 1;

-- ORDER BY with WHERE
SELECT a, b FROM test WHERE a < 13 ORDER BY b;

SELECT a, b FROM test WHERE a < 13 ORDER BY 2;

DROP TABLE test;
