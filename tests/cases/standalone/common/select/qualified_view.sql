-- Migrated from DuckDB test: test/sql/select/test_select_qualified_view.test
-- Description: Test selecting a view through a qualified reference

CREATE SCHEMA s;

-- Create table with TIME INDEX for GreptimeDB
CREATE TABLE s.a(col1 STRING, ts TIMESTAMP TIME INDEX);

INSERT INTO s.a VALUES ('hello', 1000);

-- Create view
CREATE VIEW s.b AS SELECT * FROM s.a;

-- Test schema-qualified view reference
SELECT s.b.col1 FROM s.b;

-- Test table-qualified view reference  
SELECT b.col1 FROM s.b;

-- Clean up
DROP VIEW s.b;

DROP TABLE s.a;

DROP SCHEMA s;
