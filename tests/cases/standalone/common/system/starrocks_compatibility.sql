-- Test file for StarRocks External Catalog MySQL Compatibility
-- This test simulates the exact queries StarRocks JDBC connector sends
-- Reference: MysqlSchemaResolver.java in StarRocks

-- Setup: Create test table with partitions
CREATE TABLE test_partitions (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
) PARTITION ON COLUMNS (host) ();

INSERT INTO test_partitions VALUES
    ('2024-01-01 00:00:00', 'host1', 1.0),
    ('2024-01-01 00:00:00', 'host2', 2.0);

-- ============================================
-- Section 1: JDBC DatabaseMetaData API queries
-- ============================================

-- getCatalogs() -> SHOW DATABASES
SHOW DATABASES;

-- getTables(db, null, null, types) with backtick quoting
SHOW FULL TABLES FROM `public` LIKE '%';

-- getColumns(db, null, tbl, "%") with backtick quoting
SHOW FULL COLUMNS FROM `test_partitions` FROM `public` LIKE '%';

-- ============================================
-- Section 2: INFORMATION_SCHEMA queries
-- ============================================

-- Schema listing (alternative to SHOW DATABASES)
SELECT catalog_name, schema_name FROM INFORMATION_SCHEMA.SCHEMATA
WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Tables listing
SELECT table_catalog, table_schema, table_name, table_type
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions';

-- Columns listing
SELECT table_schema, table_name, column_name, data_type, is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions'
ORDER BY ordinal_position;

-- ============================================
-- Section 3: StarRocks Partition Queries
-- These are the specific queries StarRocks sends for partition metadata
-- ============================================

-- List partition names (what StarRocks uses for partition identification)
SELECT PARTITION_DESCRIPTION as NAME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions'
  AND PARTITION_NAME IS NOT NULL
  AND (PARTITION_METHOD = 'RANGE' or PARTITION_METHOD = 'RANGE COLUMNS')
ORDER BY PARTITION_DESCRIPTION;

-- Get partition columns (StarRocks uses this to identify partition key)
SELECT DISTINCT PARTITION_EXPRESSION
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions'
  AND PARTITION_NAME IS NOT NULL
  AND (PARTITION_METHOD = 'RANGE' or PARTITION_METHOD = 'RANGE COLUMNS')
  AND PARTITION_EXPRESSION IS NOT NULL;

-- Get partitions with modification time (uses IF() function for NULL handling)
-- StarRocks uses this for cache invalidation
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) DATETIME
SELECT PARTITION_NAME,
       IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions'
  AND PARTITION_NAME IS NOT NULL
ORDER BY PARTITION_NAME;

-- Get table modification time (for non-partitioned tables, StarRocks uses this)
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) DATETIME
SELECT TABLE_NAME AS NAME,
       IF(UPDATE_TIME IS NULL, CREATE_TIME, UPDATE_TIME) AS MODIFIED_TIME
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions';

-- ============================================
-- Section 4: Raw PARTITIONS data inspection
-- Verify GreptimeDB returns appropriate partition metadata
-- ============================================

-- Show what GreptimeDB returns for PARTITIONS
-- SQLNESS REPLACE (\d{13,}) REGION_ID
SELECT table_schema, table_name, partition_name, partition_method,
       partition_expression, partition_description, greptime_partition_id
FROM INFORMATION_SCHEMA.PARTITIONS
WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = 'test_partitions';

-- ============================================
-- Section 5: IF() function tests with timestamps
-- StarRocks heavily uses IF() for NULL timestamp handling
-- ============================================

SELECT IF(1, 'yes', 'no') as result;

SELECT IF(0, 'yes', 'no') as result;

SELECT IF(NULL, 'yes', 'no') as result;

-- Cleanup
DROP TABLE test_partitions;
