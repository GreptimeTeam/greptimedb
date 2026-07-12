-- Migrated from DuckDB test: test/sql/types/float/ieee_floating_points.test
-- Test IEEE floating point behavior

-- Test special float values
CREATE TABLE float_special(f FLOAT, d DOUBLE, ts TIMESTAMP TIME INDEX);

-- Insert special values
INSERT INTO float_special VALUES
    (0.0, 0.0, 1000),
    (-0.0, -0.0, 2000),
    ('inf'::FLOAT, 'inf'::DOUBLE, 3000),
    ('-inf'::FLOAT, '-inf'::DOUBLE, 4000),
    ('nan'::FLOAT, 'nan'::DOUBLE, 5000);

-- Test basic operations with special values
SELECT f, d FROM float_special ORDER BY ts;

-- Test comparison with infinity
-- It doesn't follow the IEEE standard, but follows PG instead.
SELECT f, f > 1000000 FROM float_special ORDER BY ts;

SELECT d, d < -1000000 FROM float_special ORDER BY ts;

-- Test NaN behavior
-- NaN != NaN
SELECT f, f = f FROM float_special WHERE f != f ORDER BY ts;

SELECT d, d IS NULL FROM float_special ORDER BY ts;

-- Test arithmetic with special values
SELECT f, f + 1 FROM float_special ORDER BY ts;

SELECT d, d * 2 FROM float_special ORDER BY ts;

-- Test normal floating point precision
CREATE TABLE float_precision(f FLOAT, d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO float_precision VALUES
    (1.23456789, 1.23456789012345, 1000),
    (0.000001, 0.000000000001, 2000),
    (1e10, 1e15, 3000),
    (1e-10, 1e-15, 4000);

SELECT f, d FROM float_precision ORDER BY ts;

-- Test rounding and precision
SELECT ROUND(f, 3), ROUND(d, 6) FROM float_precision ORDER BY ts;

DROP TABLE float_special;

DROP TABLE float_precision;
