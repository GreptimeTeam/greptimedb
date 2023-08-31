CREATE TABLE test (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (11, 22, 1), (12, 21, 2), (13, 22, 3);

select b from test where a = 12;

SELECT b FROM test ORDER BY a DESC;

SELECT a, b FROM test ORDER BY a;

SELECT a, b FROM test ORDER BY a DESC;

SELECT a, b FROM test ORDER BY b, a;

SELECT a, b FROM test ORDER BY 2, 1;

SELECT a, b FROM test ORDER BY b DESC, a;

SELECT a, b FROM test ORDER BY b, a DESC;

SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1;

SELECT a, b FROM test ORDER BY b, a DESC LIMIT 1 OFFSET 1;

SELECT a, b FROM test ORDER BY b, a DESC OFFSET 1;

SELECT a, b FROM test WHERE a < 13 ORDER BY b;

SELECT a, b FROM test WHERE a < 13 ORDER BY 2;

SELECT a, b FROM test WHERE a < 13 ORDER BY b DESC;

SELECT b, a FROM test WHERE a < 13 ORDER BY b DESC;

SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY b % 2;

SELECT b % 2 AS f, a FROM test ORDER BY b % 2, a;

SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY f;

SELECT b % 2 AS f, SUM(a) FROM test GROUP BY f ORDER BY 1;

SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY k;

-- ORDER BY on alias in right-most query
-- CONTROVERSIAL: SQLite allows both "k" and "l" to be referenced here, Postgres and MonetDB give an error.
SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY l;

-- Not compatible with duckdb, work in gretimedb
SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY 1-k;

-- Not compatible with duckdb, give an error in greptimedb
-- TODO(LFC): Failed to meet the expected error:
-- expected:
--   Error: 3000(PlanQuery), Schema error: No field named 'a'. Valid fields are 'k'.
SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY a-10;

-- Not compatible with duckdb, give an error in greptimedb
-- TODO(LFC): Failed to meet the expected error:
-- expected:
--   Error: 3000(PlanQuery), Schema error: No field named 'a'. Valid fields are 'k'.
SELECT a-10 AS k FROM test UNION SELECT a-11 AS l FROM test ORDER BY a-11;

DROP TABLE test;
