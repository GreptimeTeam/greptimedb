-- Test without PK, with a windowed sort query.
CREATE TABLE test(i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs', 'compaction.twcs.max_inactive_window_files'='4');

INSERT INTO test VALUES (1, 1), (NULL, 2), (1, 3);

ADMIN FLUSH_TABLE('test');

INSERT INTO test VALUES (2, 4), (2, 5), (NULL, 6);

ADMIN FLUSH_TABLE('test');

INSERT INTO test VALUES (3, 7), (3, 8), (3, 9);

ADMIN FLUSH_TABLE('test');

INSERT INTO test VALUES (4, 10), (4, 11), (4, 12);

SELECT * FROM test ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test ORDER BY t LIMIT 5;

SELECT * FROM test ORDER BY t DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test ORDER BY t DESC LIMIT 5;

-- Filter on a field.
SELECT * FROM test where i > 2 ORDER BY t LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test where i > 2 ORDER BY t LIMIT 4;

-- Filter on a field.
SELECT * FROM test where i > 2 ORDER BY t DESC LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test where i > 2 ORDER BY t DESC LIMIT 4;

-- Filter on the time index.
SELECT * FROM test where t > 8 ORDER BY t DESC LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test where t > 8 ORDER BY t DESC LIMIT 4;

DROP TABLE test;

-- Test with PK, with a windowed sort query.
CREATE TABLE test_pk(pk INTEGER PRIMARY KEY, i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs', 'compaction.twcs.max_inactive_window_files'='4');

INSERT INTO test_pk VALUES (1, 1, 1), (2, NULL, 2), (3, 1, 3);

ADMIN FLUSH_TABLE('test_pk');

INSERT INTO test_pk VALUES (4, 2, 4), (5, 2, 5), (6, NULL, 6);

ADMIN FLUSH_TABLE('test_pk');

INSERT INTO test_pk VALUES (7, 3, 7), (8, 3, 8), (9, 3, 9);

ADMIN FLUSH_TABLE('test_pk');

INSERT INTO test_pk VALUES (10, 4, 10), (11, 4, 11), (12, 4, 12);

SELECT * FROM test_pk ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (files.*) REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT * FROM test_pk ORDER BY t LIMIT 5;

SELECT * FROM test_pk ORDER BY t DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk ORDER BY t DESC LIMIT 5;

-- Filter on a pk column.
SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (files.*) REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

DROP TABLE test_pk;
