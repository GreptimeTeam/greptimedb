CREATE TABLE single_partition(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peer-.*) REDACTED
EXPLAIN SELECT COUNT(*) FROM single_partition;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peer-.*) REDACTED
EXPLAIN SELECT SUM(i) FROM single_partition;

-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (Hash.*) REDACTED
-- SQLNESS REPLACE (peer-.*) REDACTED
EXPLAIN SELECT * FROM single_partition ORDER BY i DESC;

drop table single_partition;