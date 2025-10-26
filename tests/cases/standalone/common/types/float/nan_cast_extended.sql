-- Migrated from DuckDB test: test/sql/types/float/nan_cast.test
-- Test casting of NaN and inf values

-- Test valid casts between FLOAT, DOUBLE, and VARCHAR

-- NaN casts
SELECT 'nan'::FLOAT::DOUBLE;

SELECT 'nan'::FLOAT::VARCHAR;

SELECT 'nan'::DOUBLE::FLOAT;

SELECT 'nan'::DOUBLE::VARCHAR;

SELECT 'nan'::VARCHAR::FLOAT;

SELECT 'nan'::VARCHAR::DOUBLE;

-- Infinity casts
SELECT 'inf'::FLOAT::DOUBLE;

SELECT 'inf'::FLOAT::VARCHAR;

SELECT 'inf'::DOUBLE::FLOAT;

SELECT 'inf'::DOUBLE::VARCHAR;

SELECT 'inf'::VARCHAR::FLOAT;

SELECT 'inf'::VARCHAR::DOUBLE;

-- Negative infinity casts
SELECT '-inf'::FLOAT::DOUBLE;

SELECT '-inf'::FLOAT::VARCHAR;

SELECT '-inf'::DOUBLE::FLOAT;

SELECT '-inf'::DOUBLE::VARCHAR;

SELECT '-inf'::VARCHAR::FLOAT;

SELECT '-inf'::VARCHAR::DOUBLE;

-- Test TRY_CAST for invalid conversions (should return NULL)
SELECT TRY_CAST('nan'::FLOAT AS INTEGER);

SELECT TRY_CAST('inf'::FLOAT AS INTEGER);

SELECT TRY_CAST('-inf'::FLOAT AS INTEGER);

SELECT TRY_CAST('nan'::DOUBLE AS BIGINT);

SELECT TRY_CAST('inf'::DOUBLE AS BIGINT);

SELECT TRY_CAST('-inf'::DOUBLE AS BIGINT);

-- Test with table data
CREATE TABLE cast_test(f FLOAT, d DOUBLE, s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO cast_test VALUES
    ('nan'::FLOAT, 'nan'::DOUBLE, 'nan', 1000),
    ('inf'::FLOAT, 'inf'::DOUBLE, 'inf', 2000),
    ('-inf'::FLOAT, '-inf'::DOUBLE, '-inf', 3000),
    (1.5, 2.5, '3.5', 4000);

-- Cast between float types
SELECT f, f::DOUBLE AS fd, d, d::FLOAT AS df FROM cast_test ORDER BY ts;

-- Cast to string
SELECT f::VARCHAR, d::VARCHAR FROM cast_test ORDER BY ts;

-- Cast from string
SELECT s, TRY_CAST(s AS FLOAT) AS sf, TRY_CAST(s AS DOUBLE) AS sd FROM cast_test ORDER BY ts;

DROP TABLE cast_test;
