-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_median.test
-- Test MEDIAN aggregate

-- scalar median
SELECT median(NULL), median(1);

-- test with simple table
CREATE TABLE quantile(r INTEGER, v DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO quantile VALUES
    (0, 0.1, 1000), (1, 0.2, 2000), (2, 0.3, 3000), (3, 0.4, 4000), (4, 0.5, 5000),
    (5, 0.6, 6000), (6, 0.7, 7000), (7, 0.8, 8000), (8, 0.9, 9000), (9, 1.0, 10000),
    (NULL, 0.1, 11000), (NULL, 0.5, 12000), (NULL, 0.9, 13000);

SELECT median(r)::VARCHAR FROM quantile;

SELECT median(r::FLOAT)::VARCHAR FROM quantile;

SELECT median(r::DOUBLE)::VARCHAR FROM quantile;

SELECT median(r::SMALLINT)::VARCHAR FROM quantile WHERE r < 100;

SELECT median(r::INTEGER)::VARCHAR FROM quantile;

SELECT median(r::BIGINT)::VARCHAR FROM quantile;

-- test with NULL values
SELECT median(NULL) FROM quantile;

SELECT median(42) FROM quantile;

-- test with grouped data
CREATE TABLE median_groups(val INTEGER, grp INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO median_groups VALUES
    (1, 1, 1000), (2, 1, 2000), (3, 1, 3000), (4, 1, 4000), (5, 1, 5000),
    (10, 2, 6000), (20, 2, 7000), (30, 2, 8000), (40, 2, 9000), (50, 2, 10000),
    (NULL, 3, 11000);

SELECT grp, median(val) FROM median_groups GROUP BY grp ORDER BY grp;

-- cleanup
DROP TABLE quantile;

DROP TABLE median_groups;
