-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_approx_quantile.test
-- Test approx_percentile_cont function instead of approx_quantile

-- Test basic approximate quantile
CREATE TABLE approx_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO approx_test SELECT number, number * 1000 FROM numbers LIMIT 1000;

-- Test approx_percentile_cont
-- median
SELECT approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM approx_test;

-- first quartile
SELECT approx_percentile_cont(0.25) WITHIN GROUP (ORDER BY i) FROM approx_test;

-- third quartile
SELECT approx_percentile_cont(0.75) WITHIN GROUP (ORDER BY i) FROM approx_test;

-- 95th percentile
SELECT approx_percentile_cont(0.95) WITHIN GROUP (ORDER BY i) FROM approx_test;

-- Test approx_percentile_cont DESC
-- median
SELECT approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

-- first quartile
SELECT approx_percentile_cont(0.25) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

-- third quartile
SELECT approx_percentile_cont(0.75) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

-- 95th percentile
SELECT approx_percentile_cont(0.95) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

-- Test with different data types
CREATE TABLE approx_double(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO approx_double SELECT number * 1.5, number * 1000 FROM numbers LIMIT 1000;

SELECT approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY d) FROM approx_double;

SELECT approx_percentile_cont(0.9) WITHIN GROUP (ORDER BY d) FROM approx_double;

-- Test with groups
CREATE TABLE approx_groups(grp INTEGER, val INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO approx_groups SELECT
    number % 3 as grp,
    number,
    number * 1000
FROM numbers LIMIT 300;

SELECT grp, approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY val)
FROM approx_groups GROUP BY grp ORDER BY grp;

-- Test with NULL values
INSERT INTO approx_test VALUES (NULL, 1001000);

SELECT approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM approx_test;

-- Test edge cases
-- should be close to min
SELECT approx_percentile_cont(0.0) WITHIN GROUP (ORDER BY i) FROM approx_test;

SELECT approx_percentile_cont(1.0) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

-- should be close to max
SELECT approx_percentile_cont(1.0) WITHIN GROUP (ORDER BY i) FROM approx_test;

SELECT approx_percentile_cont(0.0) WITHIN GROUP (ORDER BY i DESC) FROM approx_test;

DROP TABLE approx_test;

DROP TABLE approx_double;

DROP TABLE approx_groups;
