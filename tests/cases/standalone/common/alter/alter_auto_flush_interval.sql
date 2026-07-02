-- Test altering auto_flush_interval on a mito table

-- Create a table without auto_flush_interval (uses global default)
CREATE TABLE test_alter_auto_flush_interval(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito;

-- Verify the table was created with the default auto_flush_interval
SHOW CREATE TABLE test_alter_auto_flush_interval;

-- Alter auto_flush_interval to 5 minutes
ALTER TABLE test_alter_auto_flush_interval SET 'auto_flush_interval' = '5m';

-- Verify the change took effect
SHOW CREATE TABLE test_alter_auto_flush_interval;

-- Insert some data after the alter
INSERT INTO test_alter_auto_flush_interval VALUES ('host1', 0, 1.0), ('host2', 1, 2.0);

SELECT * FROM test_alter_auto_flush_interval ORDER BY host, ts;

-- Re-altering to the same value should succeed
ALTER TABLE test_alter_auto_flush_interval SET 'auto_flush_interval' = '5m';

-- Re-altering to a different value should succeed
ALTER TABLE test_alter_auto_flush_interval SET 'auto_flush_interval' = '10m';

-- Verify the new value took effect
SHOW CREATE TABLE test_alter_auto_flush_interval;

-- Trying to set an invalid duration should fail
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_alter_auto_flush_interval SET 'auto_flush_interval' = 'not_a_duration';

-- Trying to set a zero duration should fail (engine requires > 0)
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_alter_auto_flush_interval SET 'auto_flush_interval' = '0s';

-- Clean up
DROP TABLE test_alter_auto_flush_interval;

-- Test altering auto_flush_interval on a table that already had it set at create time
CREATE TABLE test_alter_auto_flush_interval_with_default(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito WITH ('auto_flush_interval' = '1h');

-- Verify initial value
SHOW CREATE TABLE test_alter_auto_flush_interval_with_default;

-- Alter it
ALTER TABLE test_alter_auto_flush_interval_with_default SET 'auto_flush_interval' = '30m';

-- Verify the new value
SHOW CREATE TABLE test_alter_auto_flush_interval_with_default;

-- Clean up
DROP TABLE test_alter_auto_flush_interval_with_default;
