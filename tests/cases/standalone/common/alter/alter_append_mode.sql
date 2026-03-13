-- Test altering append_mode from false to true

-- Create a table with append_mode=false (default)
CREATE TABLE test_alter_append_mode(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito;

-- Insert some data
INSERT INTO test_alter_append_mode VALUES ('host1', 0, 1.0), ('host2', 1, 2.0);

-- Insert duplicate data (should be deduplicated since append_mode=false)
INSERT INTO test_alter_append_mode VALUES ('host1', 0, 10.0), ('host2', 1, 20.0);

-- Query should show deduplicated data (latest values)
SELECT * FROM test_alter_append_mode ORDER BY host, ts;

-- Alter append_mode from false to true
ALTER TABLE test_alter_append_mode SET 'append_mode' = 'true';

-- Verify append_mode is set via SHOW CREATE TABLE
SHOW CREATE TABLE test_alter_append_mode;

-- Insert duplicate data again (should be preserved since append_mode=true now)
INSERT INTO test_alter_append_mode VALUES ('host1', 0, 100.0), ('host2', 1, 200.0);

-- Query should show the new duplicates preserved
SELECT * FROM test_alter_append_mode ORDER BY host, ts, cpu;

-- Try to alter append_mode from true to false (should fail)
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_alter_append_mode SET 'append_mode' = 'false';

-- Clean up
DROP TABLE test_alter_append_mode;

-- Test creating with append_mode=true and trying to alter to false
CREATE TABLE test_append_mode_true(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito WITH('append_mode'='true');

-- Try to alter append_mode from true to false (should fail)
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_append_mode_true SET 'append_mode' = 'false';

-- Altering to the same value should succeed
ALTER TABLE test_append_mode_true SET 'append_mode' = 'true';

-- Clean up
DROP TABLE test_append_mode_true;

-- Test altering append_mode on a table with merge_mode set
CREATE TABLE test_alter_append_with_merge(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito WITH('merge_mode'='last_non_null');

-- Verify merge_mode is set
SHOW CREATE TABLE test_alter_append_with_merge;

-- Insert some data
INSERT INTO test_alter_append_with_merge VALUES ('host1', 0, 1.0), ('host2', 1, 2.0);

-- Alter append_mode to true (should succeed and clear merge_mode)
ALTER TABLE test_alter_append_with_merge SET 'append_mode' = 'true';

-- Verify merge_mode is cleared and append_mode is set
SHOW CREATE TABLE test_alter_append_with_merge;

-- Insert duplicate data (should be preserved since append_mode=true now)
INSERT INTO test_alter_append_with_merge VALUES ('host1', 0, 100.0), ('host2', 1, 200.0);

-- Query should show the new duplicates preserved
SELECT * FROM test_alter_append_with_merge ORDER BY host, ts, cpu;

-- Clean up
DROP TABLE test_alter_append_with_merge;
