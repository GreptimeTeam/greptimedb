-- aliasing, from:
-- https://github.com/duckdb/duckdb/blob/9196dd9b0a163e6c8aada26218803d04be30c562/test/sql/subquery/table/test_aliasing.test
CREATE TABLE a(ts TIMESTAMP TIME INDEX, i INTEGER);

insert into a values (1, 42);

SELECT * FROM (SELECT i AS j FROM a GROUP BY j) WHERE j = 42;

SELECT * FROM (SELECT i AS j FROM a GROUP BY i) WHERE j = 42;

DROP TABLE a;

-- nested table subquery, from:
-- https://github.com/duckdb/duckdb/blob/2e4e2913266ddc46c7281d1b992228cb0095954b/test/sql/subquery/table/test_nested_table_subquery.test_slow
CREATE TABLE test (ts TIMESTAMP TIME INDEX, i INTEGER, j INTEGER);

INSERT INTO test VALUES (0, 3, 4), (1, 4, 5), (2, 5, 6);

SELECT * FROM (SELECT i, j FROM (SELECT j AS i, i AS j FROM (SELECT j AS i, i AS j FROM test) AS a) AS a) AS a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;

SELECT i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM test)));

DROP TABLE test;

-- subquery union, from:
-- https://github.com/duckdb/duckdb/blob/9196dd9b0a163e6c8aada26218803d04be30c562/test/sql/subquery/table/test_subquery_union.test
SELECT * FROM (SELECT 42) UNION ALL SELECT * FROM (SELECT 43) ORDER BY 1;

-- table subquery, from:
-- https://github.com/duckdb/duckdb/blob/8704c7d0807d6ce1e2ebcdf6398e1b6cc050e507/test/sql/subquery/table/test_table_subquery.test
CREATE TABLE test (ts TIMESTAMP TIME INDEX, i INTEGER, j INTEGER);

INSERT INTO test VALUES (0, 3, 4), (1, 4, 5), (2, 5, 6);

SELECT * FROM (SELECT i, j AS d FROM test ORDER BY i) AS b;

SELECT b.d FROM (SELECT i * 2 + j AS d FROM test) AS b;

SELECT a.i,a.j,b.r,b.j FROM (SELECT i, j FROM test) AS a INNER JOIN (SELECT i+1 AS r,j FROM test) AS b ON a.i=b.r ORDER BY 1;

SELECT * FROM (SELECT i, j FROM test) AS a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;

SELECT sum(x) FROM (SELECT i AS x FROM test GROUP BY i) sq;

SELECT sum(x) FROM (SELECT i+1 AS x FROM test GROUP BY x) sq;

DROP TABLE test;

-- test unamed subquery, from:
-- https://github.com/duckdb/duckdb/blob/00a605270719941ca0412ad5d0a14b1bdfbf9eb5/test/sql/subquery/table/test_unnamed_subquery.test
SELECT a FROM (SELECT 42 a);

SELECT * FROM (SELECT 42 a), (SELECT 43 b);

SELECT * FROM (VALUES (42, 43));

SELECT * FROM (SELECT 42 a), (SELECT 43 b), (SELECT 44 c), (SELECT 45 d);

SELECT * FROM (SELECT * FROM (SELECT 42 a), (SELECT 43 b)) JOIN (SELECT 44 c) ON (true) JOIN (SELECT 45 d) ON (true);

-- skipped, unsupported feature: unnamed_subquery, see also:
-- https://github.com/GreptimeTeam/greptimedb/issues/5012
-- SELECT * FROM (SELECT unnamed_subquery.a FROM (SELECT 42 a)), (SELECT unnamed_subquery.b FROM (SELECT 43 b));
-- SELECT unnamed_subquery.a, unnamed_subquery2.b FROM (SELECT 42 a), (SELECT 43 b);
