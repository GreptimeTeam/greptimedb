-- Migrated from DuckDB test: test/sql/limit/test_preserve_insertion_order.test  
-- Description: Test advanced LIMIT functionality
-- Note: Adapted for GreptimeDB - simplified from RANGE function

-- Create test table with multiple rows
CREATE TABLE integers(i INTEGER, ts TIMESTAMP TIME INDEX);

-- Insert test data
INSERT INTO integers VALUES 
(1, 1000), (1, 2000), (1, 3000), (1, 4000), (1, 5000),
(2, 6000), (3, 7000), (4, 8000), (5, 9000), (10, 10000);

-- Test 1: Basic aggregation
SELECT MIN(i), MAX(i), COUNT(*) FROM integers;

-- Test 2: LIMIT with specific values
SELECT * FROM integers ORDER BY ts LIMIT 5;

-- Test 3: LIMIT with OFFSET  
SELECT * FROM integers ORDER BY ts LIMIT 3 OFFSET 2;

-- Test 4: LIMIT with WHERE clause
SELECT * FROM integers WHERE i IN (1, 3, 5, 10) ORDER BY i;

-- Test 5: LIMIT with WHERE and ORDER BY
SELECT * FROM integers WHERE i IN (1, 3, 5, 10) ORDER BY i LIMIT 3;

-- Clean up
DROP TABLE integers;