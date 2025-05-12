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
SELECT a-10 AS k FROM test UNION SELECT a-10 AS l FROM test ORDER BY a-10;

-- Not compatible with duckdb, give an error in greptimedb
SELECT a-10 AS k FROM test UNION SELECT a-11 AS l FROM test ORDER BY a-11;

DROP TABLE test;

-- ORDER BY for partition table
CREATE TABLE IF NOT EXISTS `t` (
  `tag` STRING NULL,
  `ts` TIMESTAMP(3) NOT NULL,
  `num` BIGINT NULL,
  TIME INDEX (`ts`),
  PRIMARY KEY (`tag`)
)
PARTITION ON COLUMNS (`tag`) (
  tag <= 'z',
  tag > 'z'
);

INSERT INTO t (tag, ts, num) VALUES
    ('abc', 0, 1),
    ('abc', 3000, 2),
    ('abc', 6000, 3),
    ('abc', 9000, 4),
    ('abc', 12000, 5),
    ('zzz', 3000, 6),
    ('zzz', 6000, 7),
    ('zzz', 9000, 8),
    ('zzz', 0, 9),
    ('zzz', 3000, 10);

select * from t where num > 3 order by ts desc limit 2;

select tag from t where num > 6 order by ts desc limit 2;

select tag from t where num > 6 order by ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
explain analyze select tag from t where num > 6 order by ts desc limit 2;

drop table t;

-- ORDER BY with projections
CREATE TABLE test (
  c1 INTEGER,
  c2 INTEGER,
  c3 STRING,
  c4 DOUBLE,
  ts TIMESTAMP TIME INDEX,
  PRIMARY KEY (c1, c3, c2)
);

INSERT INTO test VALUES (1, NULL, 'a', 3.0, 1), (2, 3, 'b', 4.0, 2), (3, 4, 'c', 5.0, 3);

SELECT c1, c3 FROM test ORDER BY c2;

SELECT c1, c3 FROM test ORDER BY c2 NULLS FIRST;

SELECT c1, c3 FROM test ORDER BY c3, c1;

SELECT c2 FROM test ORDER BY ts;

drop table test;

