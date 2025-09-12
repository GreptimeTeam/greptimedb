-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_quantile_*.test
-- Test QUANTILE functions

-- Test basic quantile
CREATE TABLE quantile_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO quantile_test VALUES
    (1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000),
    (6, 6000), (7, 7000), (8, 8000), (9, 9000), (10, 10000);

-- Test QUANTILE_CONT (continuous quantile) - replaced with APPROX_PERCENTILE_CONT
SELECT APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i) FROM quantile_test;  -- median
SELECT APPROX_PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i) FROM quantile_test; -- first quartile
SELECT APPROX_PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY i) FROM quantile_test; -- third quartile

-- Test QUANTILE_DISC (discrete quantile) - replaced with APPROX_PERCENTILE_CONT
SELECT APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i) FROM quantile_test;  -- median
SELECT APPROX_PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY i) FROM quantile_test; -- first quartile
SELECT APPROX_PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY i) FROM quantile_test; -- third quartile

-- Test with groups
CREATE TABLE quantile_groups(grp INTEGER, val DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO quantile_groups VALUES
    (1, 1.0, 1000), (1, 2.0, 2000), (1, 3.0, 3000), (1, 4.0, 4000), (1, 5.0, 5000),
    (2, 10.0, 6000), (2, 20.0, 7000), (2, 30.0, 8000), (2, 40.0, 9000), (2, 50.0, 10000);

SELECT grp, APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) as quantile_cont, 
       APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) as quantile_disc
FROM quantile_groups GROUP BY grp ORDER BY grp;

-- Test with NULL values
INSERT INTO quantile_test VALUES (NULL, 11000);

SELECT APPROX_PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i) FROM quantile_test;

-- Test edge cases
SELECT APPROX_PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY i) FROM quantile_test;  -- min
SELECT APPROX_PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY i) FROM quantile_test;  -- max

DROP TABLE quantile_test;

DROP TABLE quantile_groups;
