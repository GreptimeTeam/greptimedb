-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_covar.test
-- Test COVAR operators (covariance)

-- Test population covariance on scalar values
SELECT COVAR_POP(3,3), COVAR_POP(NULL,3), COVAR_POP(3,NULL), COVAR_POP(NULL,NULL);

-- Test sample covariance on scalar values
SELECT COVAR_SAMP(3,3), COVAR_SAMP(NULL,3), COVAR_SAMP(3,NULL), COVAR_SAMP(NULL,NULL);

-- Test population covariance on a set of values
CREATE TABLE integers(x INTEGER, y INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES
    (10, NULL, 1000),
    (10, 11, 2000),
    (20, 22, 3000),
    (25, NULL, 4000),
    (30, 35, 5000);

SELECT COVAR_POP(x,y), COVAR_POP(x,1), COVAR_POP(1,y), COVAR_POP(x,NULL), COVAR_POP(NULL,y) FROM integers;

-- Test sample covariance
SELECT COVAR_SAMP(x,y), COVAR_SAMP(x,1), COVAR_SAMP(1,y) FROM integers;

-- Test grouped covariance
CREATE TABLE covar_data(grp INTEGER, x DOUBLE, y DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO covar_data VALUES
    (1, 1.0, 2.0, 1000),
    (1, 2.0, 4.0, 2000),
    (1, 3.0, 6.0, 3000),
    (2, 10.0, 5.0, 4000),
    (2, 20.0, 10.0, 5000),
    (2, 30.0, 15.0, 6000);

SELECT grp, COVAR_POP(x, y), COVAR_SAMP(x, y) FROM covar_data GROUP BY grp ORDER BY grp;

-- cleanup
DROP TABLE integers;

DROP TABLE covar_data;
