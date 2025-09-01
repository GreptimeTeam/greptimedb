-- Migrated from DuckDB test: test/sql/error/incorrect_sql.test

-- Typo in SELECT
SELEC 42;

-- Unrecognized column
SELECT x FROM (SELECT 1 as y);

-- Unrecognized function
SELECT FUNFUNFUN();

-- Wrong aggregate parameters
SELECT SUM(42, 84, 11, 'hello');

-- No matching function signature
SELECT cos(0, 1, 2, 3);

-- Multiple WHERE clauses
SELECT 42 WHERE 1=1 WHERE 1=0;

-- Multiple statements without semicolon
SELECT 42
SELECT 42;

-- Non-existent table
SELECT * FROM integers2;

-- Non-existent schema
SELECT * FROM bla.integers2;

CREATE TABLE integers(integ INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE strings(str VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE chickens(feather INTEGER, beak INTEGER, ts TIMESTAMP TIME INDEX);

-- Non-existent column
SELECT feathe FROM chickens;

-- Non-existent column with multiple tables
SELECT feathe FROM chickens, integers, strings;

-- Ambiguous column reference
SELECT ts FROM chickens, integers;

-- Clean up
DROP TABLE chickens;

DROP TABLE strings;

DROP TABLE integers;
