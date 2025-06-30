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
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 1024, type = 'BLOOM', false_positive_rate = 0.01);

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
ALTER TABLE test MODIFY COLUMN metric SET SKIPPING INDEX WITH(granularity = 1024, type = 'BLOOM', false_positive_rate = 0.01);

-- Test queries with multiple skipping indexes
SELECT * FROM test WHERE value > 5.0 AND metric < 700 ORDER BY time;

-- SQLNESS ARG restart=true
-- Verify persistence after restart
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test modifying existing skipping index options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 8192, type = 'BLOOM', false_positive_rate = 0.01);
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test modifying existing skipping index options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 8192, type = 'BLOOM', false_positive_rate = 0.0001);
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test removing skipping index
ALTER TABLE test MODIFY COLUMN value UNSET SKIPPING INDEX;
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test adding back with different options
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 2048, type = 'BLOOM', false_positive_rate = 0.01);
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test removing all skipping indexes
ALTER TABLE test MODIFY COLUMN value UNSET SKIPPING INDEX;
ALTER TABLE test MODIFY COLUMN metric UNSET SKIPPING INDEX;
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Test invalid operations and error cases
-- Test invalid option values (should fail)
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(blabla = 1024, type = 'BLOOM');

-- Test invalid false_positive_rate values (should fail)
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 1024, type = 'BLOOM', false_positive_rate = 0);
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 1024, type = 'BLOOM', false_positive_rate = -0.01);
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 1024, type = 'BLOOM', false_positive_rate = 2);

-- Test partial options
ALTER TABLE test MODIFY COLUMN category SET SKIPPING INDEX WITH(granularity = 1024);
ALTER TABLE test MODIFY COLUMN time SET SKIPPING INDEX WITH(granularity = 1024);
ALTER TABLE test MODIFY COLUMN value SET SKIPPING INDEX WITH(granularity = 4096);
SHOW CREATE TABLE test;
SHOW INDEX FROM test;

-- Clean up
DROP TABLE test;
