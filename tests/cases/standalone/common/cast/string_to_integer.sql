-- Migrated from DuckDB test: test/sql/cast/string_to_integer_decimal_cast.test
-- Description: String to number casts
-- Note: GreptimeDB doesn't support decimal string to integer conversion

-- Valid integer string conversions
SELECT '0'::INT;

SELECT '1'::INT;

SELECT '1000000'::INT;

SELECT '-1'::INT;

SELECT '-1000'::INT;

-- Test with BIGINT
SELECT '0'::BIGINT;

SELECT '1000000'::BIGINT;

-- Convert decimal strings to DOUBLE first, then to INT if needed
SELECT '0.5'::DOUBLE;

SELECT '1.50004'::DOUBLE;

SELECT '-0.5'::DOUBLE;

-- Test invalid cases (should error)
SELECT '0.5'::INT;

SELECT 'abc'::INT;
