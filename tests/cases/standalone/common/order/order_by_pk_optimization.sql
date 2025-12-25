-- Test ORDER BY primary key optimization
-- When ordering by primary keys in ASC order, SortExec should be eliminated
-- because the storage layer already maintains data in this order.
-- Uses EXPLAIN ANALYZE to see the actual physical plan including Region-level execution.

-- Create a table with multiple primary keys
CREATE TABLE order_pk_test (
    pk_a STRING,
    pk_b STRING,
    pk_c STRING,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (pk_a, pk_b, pk_c)
);

-- Insert test data
INSERT INTO order_pk_test VALUES
    ('a1', 'b1', 'c1', 1.0, 1000),
    ('a1', 'b1', 'c2', 2.0, 2000),
    ('a1', 'b2', 'c1', 3.0, 3000),
    ('a2', 'b1', 'c1', 4.0, 4000),
    ('a2', 'b2', 'c2', 5.0, 5000);

-- Test 1: ORDER BY all primary keys in ASC order should NOT have SortExec
-- Only SortPreservingMergeExec should be present (for merging sorted partitions)
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT * FROM order_pk_test ORDER BY pk_a, pk_b, pk_c LIMIT 10;

-- Test 2: ORDER BY with DESC should have SortExec (different order from storage)
-- SQLNESS REPLACE (metrics.*) REDACTED
-- SQLNESS REPLACE (RoundRobinBatch.*) REDACTED
-- SQLNESS REPLACE (-+) -
-- SQLNESS REPLACE (\s\s+) _
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
EXPLAIN ANALYZE SELECT * FROM order_pk_test ORDER BY pk_a DESC LIMIT 10;

-- Cleanup
DROP TABLE order_pk_test;
