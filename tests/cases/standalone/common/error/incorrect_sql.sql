-- Migrated from DuckDB test: test/sql/error/incorrect_sql.test
-- Description: Test various incorrect SQL strings
-- Note: Adapted for GreptimeDB error handling validation

-- Test 1: Typo in SELECT keyword
-- ERROR
SELEC 42;

-- Test 2: Unrecognized column  
-- ERROR
SELECT x FROM (SELECT 1 as y);

-- Test 3: Unrecognized scalar function
-- ERROR
SELECT FUNFUNFUN();

-- Test 4: Incorrect aggregate parameters
-- ERROR
SELECT SUM(42, 84, 11, 'hello');

-- Test 5: No matching function signature
-- ERROR  
SELECT cos(0, 1, 2, 3);

-- Test 6: Multiple WHERE clauses
-- ERROR
SELECT 42 WHERE 1=1 WHERE 1=0;

-- Test 7: Multiple statements without semicolon
-- ERROR
SELECT 42
SELECT 42;

-- Test 8: Non-existent table
-- ERROR
SELECT * FROM integers2;

-- Test 9: Non-existent schema
-- ERROR
SELECT * FROM bla.integers2;

-- Create test tables for column error tests
CREATE TABLE integers(integ INTEGER, ts TIMESTAMP TIME INDEX);
CREATE TABLE strings(str VARCHAR, ts TIMESTAMP TIME INDEX);
CREATE TABLE chickens(feather INTEGER, beak INTEGER, ts TIMESTAMP TIME INDEX);

-- Test 10: Non-existent column
-- ERROR
SELECT feathe FROM chickens;

-- Test 11: Non-existent column with multiple tables
-- ERROR
SELECT feathe FROM chickens, integers, strings;

-- Test 12: Ambiguous column reference without qualifier
-- ERROR
SELECT ts FROM chickens, integers;

-- Clean up
DROP TABLE chickens;

DROP TABLE strings;  

DROP TABLE integers;