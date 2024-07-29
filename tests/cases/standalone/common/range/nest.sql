CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0),
    (5000,  'host1', null),
    (10000, 'host1', 1),
    (15000, 'host1', null),
    (20000, 'host1', 2),
    (0,     'host2', 3),
    (5000,  'host2', null),
    (10000, 'host2', 4),
    (15000, 'host2', null),
    (20000, 'host2', 5);

-- Test range query in nest sql

SELECT ts, host, foo FROM (SELECT ts, host, min(val) RANGE '5s' AS foo FROM host ALIGN '5s') WHERE host = 'host1' ORDER BY host, ts;

SELECT ts, b, min(c) RANGE '5s' FROM (SELECT ts, host AS b, val AS c FROM host WHERE host = 'host1') ALIGN '5s' BY (b) ORDER BY b, ts;


-- Test EXPLAIN and ANALYZE

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
EXPLAIN SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s';

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s';

DROP TABLE host;
