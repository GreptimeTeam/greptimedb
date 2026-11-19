-- Test without PK, with a windowed sort query.
CREATE TABLE test(i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs');

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
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test ORDER BY t LIMIT 5;

SELECT * FROM test ORDER BY t DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test ORDER BY t DESC LIMIT 5;

-- Filter on a field.
SELECT * FROM test where i > 2 ORDER BY t LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test where i > 2 ORDER BY t LIMIT 4;

-- Filter on a field.
SELECT * FROM test where i > 2 ORDER BY t DESC LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test where i > 2 ORDER BY t DESC LIMIT 4;

-- Filter on the time index.
SELECT * FROM test where t > 8 ORDER BY t DESC LIMIT 4;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test where t > 8 ORDER BY t DESC LIMIT 4;

DROP TABLE test;

-- Test with PK, with a windowed sort query.
CREATE TABLE test_pk(pk INTEGER PRIMARY KEY, i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs', 'compaction.twcs.trigger_file_num'='4');

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
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (files.*) REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT * FROM test_pk ORDER BY t LIMIT 5;

SELECT * FROM test_pk ORDER BY t DESC LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk ORDER BY t DESC LIMIT 5;

-- Filter on a pk column.
SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (files.*) REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE VERBOSE SELECT * FROM test_pk where pk > 7 ORDER BY t LIMIT 5;

DROP TABLE test_pk;

-- More ranges than partitions: the TopK on the time index reads all partitions
-- through one heap, and its threshold reaches the scan.
CREATE TABLE test_one_heap(i INTEGER, t TIMESTAMP TIME INDEX) WITH('compaction.type'='twcs', 'compaction.twcs.trigger_file_num'='16');

INSERT INTO test_one_heap VALUES (1, 1), (2, 2);

ADMIN FLUSH_TABLE('test_one_heap');

INSERT INTO test_one_heap VALUES (3, 3), (4, 4);

ADMIN FLUSH_TABLE('test_one_heap');

INSERT INTO test_one_heap VALUES (5, 5), (6, 6);

ADMIN FLUSH_TABLE('test_one_heap');

INSERT INTO test_one_heap VALUES (7, 7), (8, 8);

ADMIN FLUSH_TABLE('test_one_heap');

INSERT INTO test_one_heap VALUES (9, 9), (10, 10);

ADMIN FLUSH_TABLE('test_one_heap');

INSERT INTO test_one_heap VALUES (11, 11), (12, 12);

ADMIN FLUSH_TABLE('test_one_heap');

SELECT * FROM test_one_heap WHERE i > 2 ORDER BY t DESC LIMIT 3;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
-- SQLNESS REPLACE \\?"(dyn_filters|time_range|file_summary|max_part_range)\\?":.* "$1":REDACTED
EXPLAIN ANALYZE SELECT * FROM test_one_heap WHERE i > 2 ORDER BY t DESC LIMIT 3;

DROP TABLE test_one_heap;
