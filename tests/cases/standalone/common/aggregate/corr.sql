-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_corr.test
-- Test CORR operator (correlation coefficient)

-- Corner cases
SELECT corr(NULL,NULL);

-- Single value returns NULL
SELECT corr(1,1);

-- Test with table
CREATE TABLE aggr(k INT, v DECIMAL(10,2), v2 DECIMAL(10, 2), ts TIMESTAMP TIME INDEX);

INSERT INTO aggr VALUES
    (1, 10, null, 1000),
    (2, 10, 11, 2000),
    (2, 20, 22, 3000),
    (2, 25, null, 4000),
    (2, 30, 35, 5000);

SELECT k, corr(v, v2) FROM aggr GROUP BY k ORDER BY k;

SELECT corr(v, v2) FROM aggr;

-- Test with integer values
CREATE TABLE corr_test(x INTEGER, y INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO corr_test VALUES
    (1, 2, 1000),
    (2, 4, 2000),
    (3, 6, 3000),
    (4, 8, 4000),
    (5, 10, 5000);

-- Perfect positive correlation
SELECT corr(x, y) FROM corr_test;

-- Test with negative correlation
INSERT INTO corr_test VALUES
    (6, 5, 6000),
    (7, 3, 7000),
    (8, 1, 8000);

SELECT corr(x, y) FROM corr_test;

-- cleanup
DROP TABLE aggr;

DROP TABLE corr_test;
