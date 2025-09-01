-- Migrated from DuckDB test: test/sql/select/test_schema_reference.test
-- Description: Test schema reference in column reference

CREATE SCHEMA s1;

CREATE TABLE s1.tbl(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO s1.tbl VALUES (1, 1000), (2, 2000);

-- Standard schema reference: schema.table.column
SELECT s1.tbl.i FROM s1.tbl ORDER BY i;

-- Test schema mismatch error - should fail
SELECT s2.tbl.i FROM s1.tbl;

-- Clean up
DROP TABLE s1.tbl;

DROP SCHEMA s1;