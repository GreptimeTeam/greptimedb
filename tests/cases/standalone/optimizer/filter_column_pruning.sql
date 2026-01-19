-- Tests for scenarios where filter column is not in the projection (SELECT list)
-- This tests the query optimizer's ability to handle column pruning when:
-- 1. A column is used in WHERE clause but not in SELECT
-- 2. Multiple columns are used in WHERE but only subset in SELECT
-- 3. Aggregations with filters on non-projected columns

CREATE TABLE filter_prune_test (
    ts TIMESTAMP TIME INDEX,
    host STRING,
    `region` STRING,
    cpu_usage DOUBLE,
    mem_usage DOUBLE,
    disk_usage DOUBLE,
    PRIMARY KEY (host, `region`)
);

INSERT INTO filter_prune_test VALUES
    (1000, 'host1', 'us-east', 10.5, 20.0, 30.5),
    (2000, 'host1', 'us-east', 15.5, 25.0, 35.5),
    (3000, 'host1', 'us-west', 20.5, 30.0, 40.5),
    (4000, 'host2', 'us-east', 25.5, 35.0, 45.5),
    (5000, 'host2', 'us-west', 30.5, 40.0, 50.5),
    (6000, 'host3', 'eu-west', 35.5, 45.0, 55.5);

-- Basic case: filter column (host) not in SELECT projection
SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' ORDER BY ts;

-- Filter on multiple columns (host, region) but only select value columns
SELECT mem_usage, cpu_usage FROM filter_prune_test WHERE host = 'host1' AND `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT mem_usage, cpu_usage FROM filter_prune_test WHERE host = 'host1' AND `region` = 'us-east' ORDER BY ts;

-- Filter on value column (cpu_usage) not in projection
SELECT host, `region` FROM filter_prune_test WHERE cpu_usage > 20.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host, `region` FROM filter_prune_test WHERE cpu_usage > 20.0 ORDER BY ts;

-- Filter on time index but time index not in projection
SELECT host, cpu_usage FROM filter_prune_test WHERE ts > 2000 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host, cpu_usage FROM filter_prune_test WHERE ts > 2000 ORDER BY ts;

-- Complex filter: multiple columns in WHERE, only one in SELECT
SELECT mem_usage FROM filter_prune_test WHERE host = 'host1' AND cpu_usage > 10.0 AND `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT mem_usage FROM filter_prune_test WHERE host = 'host1' AND cpu_usage > 10.0 AND `region` = 'us-east' ORDER BY ts;

-- Aggregation with filter on non-projected column
SELECT SUM(cpu_usage) as total_cpu FROM filter_prune_test WHERE host = 'host1';

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT SUM(cpu_usage) as total_cpu FROM filter_prune_test WHERE host = 'host1';

-- GROUP BY with filter on column not in projection or grouping
SELECT `region`, AVG(cpu_usage) as avg_cpu FROM filter_prune_test WHERE mem_usage > 25.0 GROUP BY `region` ORDER BY `region`;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT `region`, AVG(cpu_usage) as avg_cpu FROM filter_prune_test WHERE mem_usage > 25.0 GROUP BY `region` ORDER BY `region`;

-- Filter with IN clause on non-projected column
SELECT cpu_usage FROM filter_prune_test WHERE host IN ('host1', 'host2') ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host IN ('host1', 'host2') ORDER BY ts;

-- Filter with BETWEEN on non-projected column
SELECT host FROM filter_prune_test WHERE cpu_usage BETWEEN 15.0 AND 30.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host FROM filter_prune_test WHERE cpu_usage BETWEEN 15.0 AND 30.0 ORDER BY ts;

-- Filter with LIKE on non-projected column
SELECT cpu_usage FROM filter_prune_test WHERE host LIKE 'host%' AND `region` LIKE 'us-%' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host LIKE 'host%' AND `region` LIKE 'us-%' ORDER BY ts;

-- Filter with OR conditions on non-projected columns
SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' OR `region` = 'eu-west' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' OR `region` = 'eu-west' ORDER BY ts;

-- Subquery with filter on non-projected column
SELECT cpu_usage FROM (SELECT * FROM filter_prune_test WHERE host = 'host1') sub WHERE `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM (SELECT * FROM filter_prune_test WHERE host = 'host1') sub WHERE `region` = 'us-east' ORDER BY ts;

-- Repeat with data flushed to verify column pruning works with on-disk data

ADMIN FLUSH_TABLE('filter_prune_test');

-- Basic case: filter column (host) not in SELECT projection
SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' ORDER BY ts;

-- Filter on multiple columns (host, region) but only select value columns
SELECT mem_usage, cpu_usage FROM filter_prune_test WHERE host = 'host1' AND `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT mem_usage, cpu_usage FROM filter_prune_test WHERE host = 'host1' AND `region` = 'us-east' ORDER BY ts;

-- Filter on value column (cpu_usage) not in projection
SELECT host, `region` FROM filter_prune_test WHERE cpu_usage > 20.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host, `region` FROM filter_prune_test WHERE cpu_usage > 20.0 ORDER BY ts;

-- Filter on time index but time index not in projection
SELECT host, cpu_usage FROM filter_prune_test WHERE ts > 2000 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host, cpu_usage FROM filter_prune_test WHERE ts > 2000 ORDER BY ts;

-- Complex filter: multiple columns in WHERE, only one in SELECT
SELECT mem_usage FROM filter_prune_test WHERE host = 'host1' AND cpu_usage > 10.0 AND `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT mem_usage FROM filter_prune_test WHERE host = 'host1' AND cpu_usage > 10.0 AND `region` = 'us-east' ORDER BY ts;

-- Aggregation with filter on non-projected column
SELECT SUM(cpu_usage) as total_cpu FROM filter_prune_test WHERE host = 'host1';

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT SUM(cpu_usage) as total_cpu FROM filter_prune_test WHERE host = 'host1';

-- GROUP BY with filter on column not in projection or grouping
SELECT `region`, AVG(cpu_usage) as avg_cpu FROM filter_prune_test WHERE mem_usage > 25.0 GROUP BY `region` ORDER BY `region`;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT `region`, AVG(cpu_usage) as avg_cpu FROM filter_prune_test WHERE mem_usage > 25.0 GROUP BY `region` ORDER BY `region`;

-- Filter with IN clause on non-projected column
SELECT cpu_usage FROM filter_prune_test WHERE host IN ('host1', 'host2') ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host IN ('host1', 'host2') ORDER BY ts;

-- Filter with BETWEEN on non-projected column
SELECT host FROM filter_prune_test WHERE cpu_usage BETWEEN 15.0 AND 30.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT host FROM filter_prune_test WHERE cpu_usage BETWEEN 15.0 AND 30.0 ORDER BY ts;

-- Filter with LIKE on non-projected column
SELECT cpu_usage FROM filter_prune_test WHERE host LIKE 'host%' AND `region` LIKE 'us-%' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host LIKE 'host%' AND `region` LIKE 'us-%' ORDER BY ts;

-- Filter with OR conditions on non-projected columns
SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' OR `region` = 'eu-west' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM filter_prune_test WHERE host = 'host1' OR `region` = 'eu-west' ORDER BY ts;

-- Subquery with filter on non-projected column
SELECT cpu_usage FROM (SELECT * FROM filter_prune_test WHERE host = 'host1') sub WHERE `region` = 'us-east' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT cpu_usage FROM (SELECT * FROM filter_prune_test WHERE host = 'host1') sub WHERE `region` = 'us-east' ORDER BY ts;

DROP TABLE filter_prune_test;

-- Test with data flushed to files to verify file-level column pruning
CREATE TABLE filter_prune_files (
    ts TIMESTAMP TIME INDEX,
    tag_key STRING PRIMARY KEY,
    field1 DOUBLE,
    field2 DOUBLE,
    field3 DOUBLE
);

INSERT INTO filter_prune_files VALUES
    (1000, 'a', 1.0, 2.0, 3.0),
    (2000, 'a', 4.0, 5.0, 6.0),
    (3000, 'b', 7.0, 8.0, 9.0);

ADMIN FLUSH_TABLE('filter_prune_files');

-- Second batch goes to files after flush
INSERT INTO filter_prune_files VALUES
    (4000, 'b', 10.0, 11.0, 12.0),
    (5000, 'c', 13.0, 14.0, 15.0);

ADMIN FLUSH_TABLE('filter_prune_files');

-- Third batch stays in memtable (no flush)
INSERT INTO filter_prune_files VALUES
    (6000, 'c', 16.0, 17.0, 18.0),
    (7000, 'd', 19.0, 20.0, 21.0);

-- Query after flush and additional memtable writes: filter on tag (not in projection), select only field1
SELECT field1 FROM filter_prune_files WHERE tag_key = 'a' ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT field1 FROM filter_prune_files WHERE tag_key = 'a' ORDER BY ts;

-- Query: filter on field2 (not in projection), select only field1
SELECT field1 FROM filter_prune_files WHERE field2 > 5.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT field1 FROM filter_prune_files WHERE field2 > 5.0 ORDER BY ts;

-- Complex: filter on tag and field2, select only field3
SELECT field3 FROM filter_prune_files WHERE tag_key = 'b' AND field2 > 7.0 ORDER BY ts;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE \"files\":\s\[\{.* \"file\":REDACTED
-- SQLNESS REPLACE num_ranges=\d+ num_ranges=REDACTED
EXPLAIN ANALYZE VERBOSE SELECT field3 FROM filter_prune_files WHERE tag_key = 'b' AND field2 > 7.0 ORDER BY ts;

DROP TABLE filter_prune_files;
