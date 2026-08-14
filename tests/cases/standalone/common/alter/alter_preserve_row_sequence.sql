-- Test altering preserve_row_sequence on an append-only table, including
-- persistence across a restart.

-- Create an append-only table
CREATE TABLE test_alter_preserve_row_sequence(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito WITH('append_mode'='true');

-- Insert some data
INSERT INTO test_alter_preserve_row_sequence VALUES ('host1', 0, 1.0), ('host2', 1, 2.0);

-- SET preserve_row_sequence on the append-only table should succeed
ALTER TABLE test_alter_preserve_row_sequence SET 'preserve_row_sequence' = 'true';

-- SHOW CREATE TABLE should show both append_mode and preserve_row_sequence
SHOW CREATE TABLE test_alter_preserve_row_sequence;

-- Restart the server: the option and the inserted data must survive
-- SQLNESS ARG restart=true
SHOW CREATE TABLE test_alter_preserve_row_sequence;

-- Ordinary query should still see the inserted rows after restart
SELECT * FROM test_alter_preserve_row_sequence ORDER BY host, ts;

-- UNSET preserve_row_sequence should succeed
ALTER TABLE test_alter_preserve_row_sequence UNSET 'preserve_row_sequence';

-- SHOW CREATE TABLE should keep append_mode but drop preserve_row_sequence
SHOW CREATE TABLE test_alter_preserve_row_sequence;

-- Clean up
DROP TABLE test_alter_preserve_row_sequence;

-- Test that preserve_row_sequence requires append_mode=true
CREATE TABLE test_alter_preserve_no_append(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito;

-- Setting preserve_row_sequence on a non-append-only table should fail
-- SQLNESS REPLACE \d+\(\d+,\s+\d+\) REDACTED
ALTER TABLE test_alter_preserve_no_append SET 'preserve_row_sequence' = 'true';

-- Clean up
DROP TABLE test_alter_preserve_no_append;
