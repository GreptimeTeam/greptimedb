CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- analyze at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') test;

-- 'lookback' parameter is not fully supported, the test has to be updated
-- analyze at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '1s', '2s') test;

-- analyze at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE ('1970-01-01T00:00:00'::timestamp, '1970-01-01T00:00:00'::timestamp + '10 seconds'::interval, '5s') test;

-- analyze verbose at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (Duration.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE (0, 10, '5s') test;

DROP TABLE test;

-- partition table
CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING, l STRING, PRIMARY KEY(k, l)) PARTITION ON COLUMNS (k) (k < 'a', k >= 'a');

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') test;

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE (0, 10, '5s') rate(test[10s]);

-- Test new FORMAT functionality for ANALYZE
-- analyze with JSON format
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE FORMAT JSON (0, 10, '5s') test;

-- analyze verbose with JSON format
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (Duration.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (flat_format.*) REDACTED
TQL ANALYZE VERBOSE FORMAT JSON (0, 10, '5s') test;

-- analyze with TEXT format (should be same as default)
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
TQL ANALYZE FORMAT TEXT (0, 10, '5s') test;

drop table test;

CREATE TABLE test2 (
  "greptime_timestamp" TIMESTAMP(3) NOT NULL,
  "greptime_value" DOUBLE NULL,
  "shard" STRING NULL INVERTED INDEX,
  TIME INDEX ("greptime_timestamp"),
  PRIMARY KEY ("shard")
)
PARTITION ON COLUMNS ("shard") (
  shard <= '2',
  shard > '2'
);

TQL EVAL sum(test2);

-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
TQL ANALYZE sum(test2);

DROP TABLE test2;
