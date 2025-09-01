-- Migrated from DuckDB test: test/sql/cast/boolean_autocast.test
-- Description: Test boolean casts
-- Note: GreptimeDB doesn't support automatic boolean-integer comparisons

-- Test explicit boolean casts (supported)
SELECT 1::BOOLEAN;

SELECT 0::BOOLEAN;

SELECT 'true'::BOOLEAN;

SELECT 'false'::BOOLEAN;

-- Test boolean operations
SELECT true AND false;

SELECT true OR false;

SELECT NOT true;

SELECT NOT false;

-- Test boolean comparisons (same type)
SELECT true = true;

SELECT true = false;

SELECT false = false;