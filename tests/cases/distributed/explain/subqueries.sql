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

CREATE TABLE t(ts timestamp time index, a INT, b INT);

INSERT INTO t(ts,a,b) VALUES (1,3,30),(2,1,10),(3,2,20);

CREATE TABLE t1(ts timestamp time index, a INT);

INSERT INTO t1(ts,a) VALUES (1,1),(2,3);

CREATE TABLE t2(ts timestamp time index, a INT);

INSERT INTO t2(ts,a) VALUES (1,2),(2,3);

SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expected: 1,2,3

SELECT x, COUNT(*) AS c FROM (SELECT a AS x FROM t) sq GROUP BY x ORDER BY x;
-- expected:
-- x | c
-- 1 | 1
-- 2 | 1
-- 3 | 1

SELECT DISTINCT x FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expecetd: 1,2,3

SELECT sq.x FROM (SELECT a AS x FROM t) sq ORDER BY sq.x;
-- expected: 1,2,3

SELECT y FROM (SELECT x AS y FROM (SELECT a AS x FROM t) sq1) sq2 ORDER BY y;
-- expected: 1,2,3

SELECT x, x + 1 AS y FROM (SELECT a AS x FROM t) sq ORDER BY x;
-- expected:
-- (x,y)
-- (1,2)
-- (2,3)
-- (3,4)

SELECT a FROM ((SELECT a FROM t1) UNION ALL (SELECT a FROM t2)) u ORDER BY a;
-- expected: 1,2,3,3

SELECT u1.a
FROM (SELECT a FROM t1) u1
JOIN (SELECT a FROM t2) u2 ON u1.a = u2.a
ORDER BY u1.a;
-- expected: 3

SELECT x FROM (VALUES (2),(1)) v(x) ORDER BY x;
-- expected: 1,2

SELECT x FROM (SELECT a AS x FROM t) sq ORDER BY x LIMIT 2;
-- expected: 1,2
