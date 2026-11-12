-- count(col) and count(DISTINCT col) on append-only tables must not be answered
-- from row-count statistics, while count(*) still is.
CREATE TABLE n1 (ts TIMESTAMP TIME INDEX, v BIGINT, s STRING) WITH (append_mode='true');

INSERT INTO n1 VALUES (1000, 1, 'a'), (2000, NULL, 'a'), (3000, NULL, NULL), (4000, 4, 'b');

SELECT count(*), count(v), count(s), count(DISTINCT s) FROM n1;

SELECT count(v) FROM n1;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM n1;

ADMIN FLUSH_TABLE('n1');

SELECT count(*), count(v), count(s), count(DISTINCT s) FROM n1;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM n1;

DROP TABLE n1;

CREATE TABLE n2 (
  host STRING,
  ts TIMESTAMP TIME INDEX,
  v BIGINT,
  s STRING,
  PRIMARY KEY (host)
)
PARTITION ON COLUMNS (host) (
  host < 'm',
  host >= 'm'
)
WITH (append_mode='true');

INSERT INTO n2 VALUES
  ('a', 1000, 1, 'a'),
  ('b', 2000, NULL, 'a'),
  ('x', 3000, NULL, NULL),
  ('y', 4000, 4, 'b');

-- count is pushed down to each region as a state aggregate.
SELECT count(v), count(s) FROM n2;

ADMIN FLUSH_TABLE('n2');

SELECT count(v), count(s) FROM n2;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT count(*) FROM n2;

DROP TABLE n2;
