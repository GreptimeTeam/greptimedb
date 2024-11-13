-- From https://github.com/duckdb/duckdb/tree/main/test/sql/subquery/table

-- aliasing
CREATE TABLE a(ts TIMESTAMP TIME INDEX, i INTEGER);

insert into a values (1, 42);

select * from (select i as j from a group by j) where j = 42;

select * from (select i as j from a group by i) where j = 42;

DROP TABLE a;

-- nested table subquery
CREATE TABLE test (ts TIMESTAMP TIME INDEX, i INTEGER, j INTEGER);

INSERT INTO test VALUES (0, 3, 4), (1, 4, 5), (2, 5, 6);

SELECT * FROM (SELECT i, j FROM (SELECT j AS i, i AS j FROM (SELECT j AS i, i AS j FROM test) AS a) AS a) AS a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;

SELECT i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM (SELECT i + 1 AS i FROM test)));

DROP TABLE test;

-- subquery union
SELECT * FROM (SELECT 42) UNION ALL SELECT * FROM (SELECT 43);

-- table subquery
CREATE TABLE test (ts TIMESTAMP TIME INDEX, i INTEGER, j INTEGER);

INSERT INTO test VALUES (0, 3, 4), (1, 4, 5), (2, 5, 6);

SELECT * FROM (SELECT i, j AS d FROM test ORDER BY i) AS b;

SELECT b.d FROM (SELECT i * 2 + j AS d FROM test) AS b;

SELECT a.i,a.j,b.r,b.j FROM (SELECT i, j FROM test) AS a INNER JOIN (SELECT i+1 AS r,j FROM test) AS b ON a.i=b.r ORDER BY 1;

SELECT * FROM (SELECT i, j FROM test) AS a, (SELECT i+1 AS r,j FROM test) AS b, test WHERE a.i=b.r AND test.j=a.i ORDER BY 1;

DROP TABLE test;

-- test unamed subquery
SELECT * FROM (SELECT 42);

SELECT * FROM (SELECT 42), (SELECT 43);

SELECT * FROM (VALUES (42, 43));

SELECT * FROM (SELECT 42), (SELECT 43), (SELECT 44), (SELECT 45);

SELECT * FROM (SELECT * FROM (SELECT 42), (SELECT 43)) JOIN (SELECT 44) ON (true) JOIN (SELECT 45) ON (true);
