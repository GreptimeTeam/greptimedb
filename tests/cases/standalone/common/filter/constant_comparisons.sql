-- Migrated from DuckDB test: test/sql/filter/test_constant_comparisons.test
-- Description: Test expressions with constant comparisons

CREATE TABLE integers(a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (2, 12, 1000);

-- Test various constant comparisons
SELECT * FROM integers WHERE 2=2;

SELECT * FROM integers WHERE 2=3;

SELECT * FROM integers WHERE 2<>3;

SELECT * FROM integers WHERE 2<>2;

SELECT * FROM integers WHERE 2>1;

SELECT * FROM integers WHERE 2>2;

SELECT * FROM integers WHERE 2>=2;

SELECT * FROM integers WHERE 2>=3;

SELECT * FROM integers WHERE 2<3;

SELECT * FROM integers WHERE 2<2;

SELECT * FROM integers WHERE 2<=2;

SELECT * FROM integers WHERE 2<=1;

-- NULL comparisons
SELECT a=NULL FROM integers;

SELECT NULL=a FROM integers;

-- IN clause with constants
SELECT * FROM integers WHERE 2 IN (2, 3, 4, 5);

SELECT * FROM integers WHERE 2 IN (1, 3, 4, 5);

-- Clean up
DROP TABLE integers;
