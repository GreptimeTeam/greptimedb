-- Migrated from DuckDB test: test/sql/types/float/nan_arithmetic.test
-- Test arithmetic on NaN values

-- Test NaN arithmetic with FLOAT
-- Any arithmetic on a NaN value will result in a NaN value

SELECT 'nan'::FLOAT + 1;

SELECT 'nan'::FLOAT + 'inf'::FLOAT;

SELECT 'nan'::FLOAT - 1;

SELECT 'nan'::FLOAT - 'inf'::FLOAT;

SELECT 'nan'::FLOAT * 1;

SELECT 'nan'::FLOAT * 'inf'::FLOAT;

SELECT 'nan'::FLOAT / 1;

SELECT 'nan'::FLOAT / 'inf'::FLOAT;

SELECT 'nan'::FLOAT % 1;

SELECT 'nan'::FLOAT % 'inf'::FLOAT;

SELECT -('nan'::FLOAT);

-- Test NaN arithmetic with DOUBLE
SELECT 'nan'::DOUBLE + 1;

SELECT 'nan'::DOUBLE + 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE - 1;

SELECT 'nan'::DOUBLE - 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE * 1;

SELECT 'nan'::DOUBLE * 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE / 1;

SELECT 'nan'::DOUBLE / 'inf'::DOUBLE;

SELECT 'nan'::DOUBLE % 1;

SELECT 'nan'::DOUBLE % 'inf'::DOUBLE;

SELECT -('nan'::DOUBLE);

-- Test infinity arithmetic
SELECT 'inf'::FLOAT + 1;

SELECT 'inf'::FLOAT - 1;

SELECT 'inf'::FLOAT * 2;

SELECT 'inf'::FLOAT / 2;

SELECT -('inf'::FLOAT);

SELECT 'inf'::DOUBLE + 1;

SELECT 'inf'::DOUBLE - 1;

SELECT 'inf'::DOUBLE * 2;

SELECT 'inf'::DOUBLE / 2;

SELECT -('inf'::DOUBLE);

-- Test special infinity cases
-- Should be NaN
SELECT 'inf'::FLOAT - 'inf'::FLOAT;

-- Should be NaN
SELECT 'inf'::FLOAT / 'inf'::FLOAT;

-- Should be NaN
SELECT 'inf'::FLOAT * 0;

-- Should be NaN
SELECT 'inf'::DOUBLE - 'inf'::DOUBLE;

-- Should be NaN
SELECT 'inf'::DOUBLE / 'inf'::DOUBLE;

-- Should be NaN
SELECT 'inf'::DOUBLE * 0;

