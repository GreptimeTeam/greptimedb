-- Migrated from DuckDB test: test/sql/select/test_multi_column_reference.test
-- Description: Test multi column reference
-- Note: Adapted for GreptimeDB - focusing on schema.table.column references

-- Test schema -> table -> column reference
CREATE SCHEMA test;

CREATE TABLE test.tbl(col INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test.tbl VALUES (1, 1000), (2, 2000), (3, 3000);

-- Full qualified reference: schema.table.column
SELECT test.tbl.col FROM test.tbl ORDER BY col;

-- Table qualified reference
SELECT tbl.col FROM test.tbl ORDER BY col;

-- Simple column reference
SELECT col FROM test.tbl ORDER BY col;

-- Test with table alias
SELECT t.col FROM test.tbl t ORDER BY col;

-- Note: DuckDB's struct field access (t.t.t pattern) is not applicable to GreptimeDB
-- as GreptimeDB doesn't support ROW/STRUCT types in the same way

-- Clean up
DROP TABLE test.tbl;

DROP SCHEMA test;
