-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_regression.test
-- Test REGRESSION functions

-- Test REGR_SLOPE, REGR_INTERCEPT, REGR_R2
CREATE TABLE regr_test(x DOUBLE, y DOUBLE, ts TIMESTAMP TIME INDEX);

-- Linear relationship: y = 2x + 1
INSERT INTO regr_test VALUES
    (1.0, 3.0, 1000), (2.0, 5.0, 2000), (3.0, 7.0, 3000),
    (4.0, 9.0, 4000), (5.0, 11.0, 5000);

-- Test regression slope (should be close to 2)
SELECT REGR_SLOPE(y, x) FROM regr_test;

-- Test regression intercept (should be close to 1)
SELECT REGR_INTERCEPT(y, x) FROM regr_test;

-- Test R-squared (should be close to 1 for perfect fit)
SELECT REGR_R2(y, x) FROM regr_test;

-- Test REGR_COUNT (number of non-null pairs)
SELECT REGR_COUNT(y, x) FROM regr_test;

-- Test REGR_SXX, REGR_SYY, REGR_SXY
SELECT REGR_SXX(y, x), REGR_SYY(y, x), REGR_SXY(y, x) FROM regr_test;

-- Test REGR_AVGX, REGR_AVGY
SELECT REGR_AVGX(y, x), REGR_AVGY(y, x) FROM regr_test;

-- Test with noisy data
CREATE TABLE regr_noisy(x DOUBLE, y DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO regr_noisy VALUES
    (1.0, 3.1, 1000), (2.0, 4.9, 2000), (3.0, 7.2, 3000),
    (4.0, 8.8, 4000), (5.0, 11.1, 5000);

SELECT REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x) FROM regr_noisy;

-- Test with groups
CREATE TABLE regr_groups(grp INTEGER, x DOUBLE, y DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO regr_groups VALUES
    (1, 1.0, 3.0, 1000), (1, 2.0, 5.0, 2000), (1, 3.0, 7.0, 3000),
    (2, 1.0, 2.0, 4000), (2, 2.0, 4.0, 5000), (2, 3.0, 6.0, 6000);

SELECT grp, REGR_SLOPE(y, x), REGR_INTERCEPT(y, x), REGR_R2(y, x)
FROM regr_groups GROUP BY grp ORDER BY grp;

DROP TABLE regr_test;

DROP TABLE regr_noisy;

DROP TABLE regr_groups;
