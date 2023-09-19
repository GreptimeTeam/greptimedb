CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- insert two points at 1ms and one point at 2ms
INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");

-- explain at 0s, 5s and 10s. No point at 0s.
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
TQL EXPLAIN (0, 10, '5s') test;

DROP TABLE test;
