-- Migrated from DuckDB test: test/sql/subquery/test_offset.test
-- Description: Test bound offset in subquery

-- Test with VALUES clause equivalent using a simple table
CREATE TABLE temp_values(c0 INTEGER, ts TIMESTAMP TIME INDEX);
INSERT INTO temp_values VALUES (1, 1);

SELECT (SELECT c0 FROM temp_values OFFSET 1) as result;

-- Test with actual data
SELECT (SELECT c0 FROM temp_values OFFSET 0) as result;

-- Test with multiple rows
INSERT INTO temp_values VALUES (2, 2), (3, 3);

SELECT (SELECT c0 FROM temp_values ORDER BY c0 OFFSET 1 LIMIT 1) as result;

-- Clean up
DROP TABLE temp_values;
