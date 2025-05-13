explain select * from numbers;

explain select * from numbers order by number desc;

explain select * from numbers order by number asc;

explain select * from numbers order by number desc limit 10;

explain select * from numbers order by number asc limit 10;

CREATE TABLE test_pk(pk INTEGER PRIMARY KEY, i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs', 'compaction.twcs.max_inactive_window_files'='4');

INSERT INTO test_pk VALUES (1, 1, 1), (2, NULL, 2), (3, 1, 3), (4, 2, 4), (5, 2, 5), (6, NULL, 6);

-- Test aliasing.
SELECT i, t AS alias_ts FROM test_pk ORDER BY t DESC LIMIT 5;

-- Test aliasing.
SELECT i, t AS alias_ts FROM test_pk ORDER BY alias_ts DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT i, t AS alias_ts FROM test_pk ORDER BY t DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT i, t AS alias_ts FROM test_pk ORDER BY alias_ts DESC LIMIT 5;

DROP TABLE test_pk;
