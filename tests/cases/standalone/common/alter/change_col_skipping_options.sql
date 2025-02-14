 -- Test basic skipping index operations on a single column
CREATE TABLE `test` (
  `value` DOUBLE,
  `category` STRING,
  `metric` INT64,
  `time` TIMESTAMP TIME INDEX,
);

SHOW CREATE TABLE test;

-- Write initial data
INSERT INTO test VALUES 
(1.0, 'A', 100, '2020-01-01 00:00:00'),
(2.0, 'B', 200, '2020-01-01 00:00:01'),
(3.0, 'A', 300, '2020-01-02 00:00:00'),
(4.0, 'B', 400, '2020-01-02 00:00:01');

-- Test queries before adding skipping index
SELECT * FROM test WHERE value > 2.0 ORDER BY time;
SELECT * FROM test WHERE metric > 200 ORDER BY time;

-- Add skipping index
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 1024, index_type = 'BLOOM');

-- Test queries after adding skipping index
SELECT * FROM test WHERE value > 2.0 ORDER BY time;
SELECT * FROM test WHERE value BETWEEN 2.0 AND 4.0 ORDER BY time;

-- Add more data to test dynamic updates
INSERT INTO test VALUES 
(5.0, 'C', 500, '2020-01-03 00:00:00'),
(6.0, 'A', 600, '2020-01-03 00:00:01'),
(7.0, 'B', 700, '2020-01-04 00:00:00'),
(8.0, 'C', 800, '2020-01-04 00:00:01');

-- Test queries with new data
SELECT * FROM test WHERE value > 6.0 ORDER BY time;
SELECT * FROM test WHERE value < 3.0 ORDER BY time;

-- Test multiple columns with skipping indexes
ALTER TABLE test MODIFY COLUMN metric SET SKIPPING INDEX WITH(granularity = 1024, index_type = 'BLOOM');

-- Test queries with multiple skipping indexes
SELECT * FROM test WHERE value > 5.0 AND metric < 700 ORDER BY time;

-- SQLNESS ARG restart=true
-- Verify persistence after restart
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test modifying existing skipping index options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 8192, index_type = 'BLOOM');
SHOW CREATE TABLE test;

-- Test removing skipping index
ALTER TABLE test MODIFY COLUMN value UNSET SKIPPING INDEX;
SHOW INDEX FROM test;

-- Test adding back with different options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 2048, index_type = 'BLOOM');
SHOW CREATE TABLE test;

-- Test removing all skipping indexes
ALTER TABLE test MODIFY COLUMN value UNSET SKIPPING INDEX;
ALTER TABLE test MODIFY COLUMN metric UNSET SKIPPING INDEX;
SHOW INDEX FROM test;

-- Test invalid operations and error cases
-- Try to set skipping index on string column (should fail)
ALTER TABLE test MODIFY COLUMN category SET SKIPPING INDEX WITH(granularity = 1024, index_type = 'BLOOM');

-- Try to set skipping index on timestamp column (should fail)
ALTER TABLE test MODIFY COLUMN time SET SKIPPING INDEX WITH(granularity = 1024, index_type = 'BLOOM');

-- Test invalid option values
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(blabla = 1024, index_type = 'wrong_type');

-- Test partial options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 4096);
SHOW INDEX FROM test;

-- Clean up
DROP TABLE test;
