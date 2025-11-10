-- Migrated from DuckDB test: test/sql/types/float/infinity_test.test, nan_aggregate.test
-- Test infinity and NaN handling
-- Note: it doesn't follow the IEEE standard, but follows PG instead: https://www.postgresql.org/docs/current/datatype-numeric.html
-- Test infinity operations
CREATE TABLE inf_test(val DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO inf_test VALUES
    ('inf'::DOUBLE, 1000),
    ('-inf'::DOUBLE, 2000),
    (1.0, 3000),
    (-1.0, 4000),
    (0.0, 5000);

-- Test infinity comparisons
SELECT val, val > 0 FROM inf_test ORDER BY ts;

SELECT val, val < 0 FROM inf_test ORDER BY ts;

SELECT val, val = 'inf'::DOUBLE FROM inf_test ORDER BY ts;

-- Test infinity in aggregates
SELECT MAX(val), MIN(val) FROM inf_test;

SELECT SUM(val), AVG(val) FROM inf_test;

-- Test NaN behavior
CREATE TABLE nan_test(val DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO nan_test VALUES
    ('nan'::DOUBLE, 1000),
    (1.0, 2000),
    (2.0, 3000),
    ('nan'::DOUBLE, 4000),
    (3.0, 5000);

-- Test NaN in aggregates
SELECT COUNT(*), COUNT(val) FROM nan_test;

SELECT MAX(val), MIN(val) FROM nan_test;

SELECT SUM(val), AVG(val) FROM nan_test;

-- Test NaN comparisons
SELECT val, val = val FROM nan_test ORDER BY ts;

SELECT val, val IS NULL FROM nan_test ORDER BY ts;

-- Test arithmetic with infinity and NaN
SELECT 'inf'::DOUBLE + 1;

SELECT 'inf'::DOUBLE - 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE * 0;

SELECT 'nan'::DOUBLE + 1;

SELECT 'nan'::DOUBLE * 0;

DROP TABLE inf_test;

DROP TABLE nan_test;
