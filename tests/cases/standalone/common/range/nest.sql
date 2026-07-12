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

CREATE TABLE host_union_0 (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
  ts2 timestamp(3),
);

CREATE TABLE host_union_1 (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
  ts2 timestamp(3),
);

INSERT INTO TABLE host_union_0 VALUES
    (0,     'host1', 3, 0),
    (5000,  'host1', 2, 5000),
    (10000, 'host1', 1, 10000);

INSERT INTO TABLE host_union_1 VALUES
    (0,     'host1', 6, 0),
    (5000,  'host1', 5, 5000),
    (10000, 'host1', 4, 10000);

SELECT ts, host, min(val ORDER BY ts ASC) RANGE '5s'
FROM (
  SELECT ts, host, val, ts2 FROM host_union_0
  UNION ALL
  SELECT ts, host, val, ts2 FROM host_union_1
)
WHERE ts >= '1970-01-01 00:00:00'
ALIGN '5s' BY (host)
ORDER BY host, ts;

SELECT tmp.ts, tmp.host, min(tmp.val ORDER BY tmp.ts ASC) RANGE '5s'
FROM (
  SELECT ts, host, val, ts2 FROM host_union_0
  UNION ALL
  SELECT ts, host, val, ts2 FROM host_union_1
) AS tmp
WHERE tmp.ts >= '1970-01-01 00:00:00'
ALIGN '5s' BY (tmp.host)
ORDER BY tmp.host, tmp.ts;

DROP TABLE host_union_0;

DROP TABLE host_union_1;


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
