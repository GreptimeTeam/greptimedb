CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '5s') test;

-- 'lookback' parameter is not fully supported, the test has to be updated
-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '1s', '2s') test;

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN ('1970-01-01T00:00:00'::timestamp, '1970-01-01T00:00:00'::timestamp + '10 seconds'::interval, '5s') test;

-- explain verbose at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN VERBOSE (0, 10, '5s') test;

-- explain verbose at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN VERBOSE (0, 10, '5s') test AS series;

CREATE TABLE test_nano(i DOUBLE, j TIMESTAMP(9) TIME INDEX, k STRING PRIMARY KEY);

INSERT INTO test_nano VALUES (1, 1000000, "a"), (1, 1000000, "b"), (2, 2000000, "a");

-- explain at 0s, 5s and 10s for a nanosecond time index.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN (0, 10, '5s') test_nano;

-- explain verbose at 0s, 5s and 10s for a nanosecond time index.
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (elapsed_compute.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (RepartitionExec:.*) RepartitionExec: REDACTED
TQL EXPLAIN VERBOSE (0, 10, '5s') test_nano;

DROP TABLE test_nano;

DROP TABLE test;
