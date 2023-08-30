CREATE TABLE test (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (11, 22, 1), (12, 21, 2), (13, 22, 3);

SELECT a FROM test ORDER BY 2;

-- Not work in greptimedb
SELECT a FROM test ORDER BY 'hello', a;

-- Ambiguous reference in union alias, give and error in duckdb, but works in greptimedb
SELECT a AS k, b FROM test UNION SELECT a, b AS k FROM test ORDER BY k;

SELECT a AS k, b FROM test UNION SELECT a AS k, b FROM test ORDER BY k;

SELECT a % 2, b FROM test UNION SELECT b, a % 2 AS k ORDER BY a % 2;

-- Works duckdb, but not work in greptimedb
-- TODO(LFC): Failed to meet the expected error:
-- expected:
--   Error: 3000(PlanQuery), Schema error: No field named 'a'. Valid fields are 'test.a % Int64(2)', 'b'.
SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY a % 2;

SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY 3;

SELECT a % 2, b FROM test UNION SELECT a % 2 AS k, b FROM test ORDER BY -1;

SELECT a % 2, b FROM test UNION SELECT a % 2 AS k FROM test ORDER BY -1;

DROP TABLE test;
