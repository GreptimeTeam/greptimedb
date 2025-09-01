-- Migrated from DuckDB test: test/sql/overflow/integer_overflow.test
-- Description: Test handling of integer overflow in arithmetic

-- TINYINT addition tests
SELECT 100::TINYINT + 1::TINYINT;

-- Should cause overflow
SELECT 100::TINYINT + 50::TINYINT;

-- Near boundary case
SELECT 0::TINYINT + (-127)::TINYINT;

-- Should cause underflow
SELECT (-2)::TINYINT + (-127)::TINYINT;

-- SMALLINT addition tests
SELECT 30000::SMALLINT + 1::SMALLINT;

-- Should cause overflow
SELECT 30000::SMALLINT + 5000::SMALLINT;

-- Near boundary case
SELECT 0::SMALLINT + (-32767)::SMALLINT;

-- Should cause underflow
SELECT (-2)::SMALLINT + (-32767)::SMALLINT;

-- INTEGER addition tests
SELECT 2147483640::INTEGER + 1::INTEGER;

-- Should cause overflow
SELECT 2147483640::INTEGER + 5000::INTEGER;

-- BIGINT addition tests (near max value)
SELECT 9223372036854775800::BIGINT + 1::BIGINT;

-- Should cause overflow
SELECT 9223372036854775800::BIGINT + 1000::BIGINT;