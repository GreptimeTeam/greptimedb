-- Migrated from DuckDB test: test/sql/keywords/keywords_in_expressions.test
-- Description: Test keywords in expressions
-- Note: Adapted for GreptimeDB - simplified from ENUM/ROW to basic types

-- Create schema with quoted keywords
CREATE SCHEMA "SCHEMA";

-- Create table with quoted keyword names
CREATE TABLE "SCHEMA"."TABLE"("COLUMN" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO "SCHEMA"."TABLE" VALUES ('ALL', 1000);

-- Test 1: Column references with quoted keywords
SELECT "COLUMN" FROM "SCHEMA"."TABLE";

-- Test 2: Table-qualified column reference
SELECT "TABLE"."COLUMN" FROM "SCHEMA"."TABLE";

-- Test 3: Schema-qualified column reference
SELECT "SCHEMA"."TABLE"."COLUMN" FROM "SCHEMA"."TABLE";

-- Clean up
DROP TABLE "SCHEMA"."TABLE";
DROP SCHEMA "SCHEMA";

-- Test with common reserved keywords
CREATE TABLE "SELECT"("WHERE" INTEGER, "FROM" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO "SELECT" VALUES (1, 'test', 2000);

-- Test 4: References to table/columns with reserved keywords
SELECT "WHERE", "FROM" FROM "SELECT";

-- Test 5: Qualified references with reserved keywords
SELECT "SELECT"."WHERE", "SELECT"."FROM" FROM "SELECT";

-- Clean up
DROP TABLE "SELECT";