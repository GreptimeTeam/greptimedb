CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers WHERE i IN ((SELECT i FROM integers)) ORDER BY i;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

create table other (i INTEGER, j TIMESTAMP TIME INDEX);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
explain select t.i
from (
    select * from integers join other on 1=1
) t
where t.i is not null
order by t.i desc;

INSERT INTO other SELECT i, 2 FROM integers WHERE i=(SELECT MAX(i) FROM integers);

-- Explain physical plan for DML is not supported because it looks up the table name in a way that is
-- different from normal queries. It also requires the table provider to implement the `insert_into()` method.
EXPLAIN INSERT INTO other SELECT i, 2 FROM integers WHERE i=(SELECT MAX(i) FROM integers);

drop table other;

drop table integers;

CREATE TABLE integers(i INTEGER, j TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (i) (
  i < 1000,
  i >= 1000 AND i < 2000,
  i >= 2000
);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT i FROM integers WHERE i=i1.i) ORDER BY i1.i;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT * FROM integers i1 WHERE EXISTS(SELECT count(i) FROM integers WHERE i=i1.i) ORDER BY i1.i;

DROP TABLE integers;

CREATE TABLE t(ts timestamp time index, a INT, b INT)PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

CREATE TABLE t1(ts timestamp time index, a INT)PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

CREATE TABLE t2(ts timestamp time index, a INT)PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO t(ts,a,b) VALUES (1,3,30),(2,1,10),(3,2,20);

INSERT INTO t1(ts,a) VALUES (1,1),(2,3);

INSERT INTO t2(ts,a) VALUES (1,2),(2,3);

SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expected: 1,2,3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x;

SELECT x, COUNT(*) AS c FROM (SELECT a AS x FROM t) sq GROUP BY x ORDER BY x;
-- expected:
-- x | c
-- 1 | 1
-- 2 | 1
-- 3 | 1

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT x, COUNT(*) AS c FROM (SELECT a AS x FROM t) sq GROUP BY x ORDER BY x;

SELECT DISTINCT x FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expecetd: 1,2,3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT DISTINCT x FROM (SELECT a AS x FROM t) sq ORDER BY x;

SELECT sq.x FROM (SELECT a AS x FROM t) sq ORDER BY sq.x;
-- expected: 1,2,3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT sq.x FROM (SELECT a AS x FROM t) sq ORDER BY sq.x;

SELECT y FROM (SELECT x AS y FROM (SELECT a AS x FROM t) sq1) sq2 ORDER BY y;
-- expected: 1,2,3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT y FROM (SELECT x AS y FROM (SELECT a AS x FROM t) sq1) sq2 ORDER BY y;

SELECT x, x + 1 AS y FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expected:
-- (x,y)
-- (1,2)
-- (2,3)
-- (3,4)

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT x, x + 1 AS y FROM (SELECT a AS x FROM t) sq ORDER BY x;

SELECT a FROM ((SELECT a FROM t1) UNION ALL (SELECT a FROM t2)) u ORDER BY a;
-- expected: 1,2,3,3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT a FROM ((SELECT a FROM t1) UNION ALL (SELECT a FROM t2)) u ORDER BY a;

SELECT u1.a
FROM (SELECT a FROM t1) u1
JOIN (SELECT a FROM t2) u2 ON u1.a = u2.a
ORDER BY u1.a;
-- expected: 3

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT u1.a
FROM (SELECT a FROM t1) u1
JOIN (SELECT a FROM t2) u2 ON u1.a = u2.a
ORDER BY u1.a;

SELECT x FROM (VALUES (2),(1)) v(x) ORDER BY x;
-- expected: 1,2

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT x FROM (VALUES (2),(1)) v(x) ORDER BY x;

SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x LIMIT 2;
-- expected: 1,2

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x LIMIT 2;

DROP TABLE t;
DROP TABLE t1;
DROP TABLE t2;

-- Regression test for https://github.com/GreptimeTeam/greptimedb/issues/9260:
-- nested scalar subqueries must get a MergeScan inside every subquery level.
CREATE TABLE nested_scalar (k INT, v BIGINT, ts TIMESTAMP TIME INDEX);

INSERT INTO nested_scalar VALUES (1, 10, '2024-01-01 00:00:00'), (2, NULL, '2024-01-01 00:00:01');

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT (SELECT (SELECT MAX(v) FROM nested_scalar)) AS m FROM nested_scalar LIMIT 1;

SELECT (SELECT MAX(v) FROM nested_scalar WHERE v > (SELECT MAX(v) FROM nested_scalar)) AS m FROM nested_scalar LIMIT 1;

SELECT (SELECT (SELECT MAX(v) FROM nested_scalar)) AS m FROM nested_scalar LIMIT 1;

DROP TABLE nested_scalar;

-- Same regression as above, but on a range-partitioned table so the inner
-- aggregate must merge partial results from multiple regions (the global
-- AVG must be computed across regions before the outer SUM filter runs).
CREATE TABLE nested_scalar_part (k INT, v BIGINT, ts TIMESTAMP TIME INDEX)
PARTITION ON COLUMNS (k) (
  k < 20,
  k >= 20 AND k < 40,
  k >= 40
);

INSERT INTO nested_scalar_part VALUES
  (1, 10, '2024-01-01 00:00:00'),
  (2, NULL, '2024-01-01 00:00:01'),
  (21, 20, '2024-01-01 00:00:02'),
  (25, 30, '2024-01-01 00:00:03'),
  (41, 40, '2024-01-01 00:00:04'),
  (50, 50, '2024-01-01 00:00:05');

-- global AVG(v) = 30, so the outer SUM(v) must be 40 + 50 = 90
SELECT (SELECT SUM(v) FROM nested_scalar_part WHERE v > (SELECT AVG(v) FROM nested_scalar_part)) AS m FROM nested_scalar_part LIMIT 1;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT (SELECT SUM(v) FROM nested_scalar_part WHERE v > (SELECT AVG(v) FROM nested_scalar_part)) AS m FROM nested_scalar_part LIMIT 1;

DROP TABLE nested_scalar_part;
