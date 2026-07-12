CREATE TABLE test (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (11, 22, 1), (12, 21, 2), (13, 22, 3);

SELECT a FROM test ORDER BY 2;

SELECT a FROM test ORDER BY 'hello', a;

-- Ambiguous reference in union alias, give and error in duckdb, but works in greptimedb
SELECT a AS k, b FROM test UNION SELECT a, b AS k FROM test ORDER BY k;

SELECT a AS k, b FROM test UNION SELECT a AS k, b FROM test ORDER BY k;

SELECT a % 2, b FROM test UNION SELECT b, a % 2 AS k ORDER BY a % 2;

-- Works duckdb, but not work in greptimedb
SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY a % 2;

SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY 3;

-- "order by -1" is generally an undefined behavior.
-- It's not supported in PostgreSQL 16, error "ORDER BY position -1 is not in select list".
-- But in Mysql 8, it can be executed, just the actual order is ignored.
-- In DataFusion, it behaves like Mysql 8. The "sort" plan node will be eliminated by the physical optimizer
-- "EnforceSorting" because it's sort key is parsed as a constant "-1".
-- We check the "explain" of the "order by -1" query to ensure that.
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY -1;

SELECT a % 2, b FROM test UNION SELECT a % 2 AS k FROM test ORDER BY -1;

DROP TABLE test;
