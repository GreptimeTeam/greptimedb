-- Migrated from DuckDB test: test/sql/overflow/integer_overflow.test
-- Note: GreptimeDB wraps on overflow, DuckDB throws error

-- TINYINT addition tests
SELECT 100::TINYINT + 1::TINYINT;

SELECT 100::TINYINT + 50::TINYINT;  -- overflow: wraps to -106

SELECT 0::TINYINT + (-127)::TINYINT;

SELECT (-2)::TINYINT + (-127)::TINYINT;  -- underflow: wraps to 127

-- SMALLINT addition tests
SELECT 30000::SMALLINT + 1::SMALLINT;

SELECT 30000::SMALLINT + 5000::SMALLINT;  -- overflow: wraps to -30536

SELECT 0::SMALLINT + (-32767)::SMALLINT;

SELECT (-2)::SMALLINT + (-32767)::SMALLINT;  -- underflow: wraps to 32767

-- INTEGER addition tests
SELECT 2147483640::INTEGER + 1::INTEGER;

SELECT 2147483640::INTEGER + 5000::INTEGER;  -- overflow: wraps

-- BIGINT addition tests
SELECT 9223372036854775800::BIGINT + 1::BIGINT;

SELECT 9223372036854775800::BIGINT + 1000::BIGINT;  -- overflow: wraps
