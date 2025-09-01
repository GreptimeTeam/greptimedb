-- Migrated from DuckDB test: test/sql/select/test_projection_names.test
-- Description: Test projection lists
-- Note: Adapted for GreptimeDB - testing column selection and naming

CREATE TABLE integers(COL1 INTEGER, COL2 INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 10, 1000), (2, 20, 2000);

-- Test 1: SELECT * projection
SELECT * FROM integers ORDER BY ts;

-- Test 2: Explicit column projection
SELECT COL1, COL2 FROM integers ORDER BY COL1;

-- Test 3: Table-qualified column reference
SELECT integers.COL1, integers.COL2 FROM integers ORDER BY COL1;

-- Test 4: Column alias
SELECT COL1 as c1, COL2 as c2 FROM integers ORDER BY c1;

-- Test 5: Mixed qualified and unqualified references
SELECT integers.COL1, COL2 FROM integers ORDER BY COL1;

-- Clean up
DROP TABLE integers;