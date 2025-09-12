-- Migrated from DuckDB test style: test boolean aggregation
-- Test BOOL_AND and BOOL_OR functions

-- Test with boolean values
CREATE TABLE bool_test(b BOOLEAN, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO bool_test VALUES
    (true, 1, 1000), (true, 1, 2000), (true, 1, 3000),
    (false, 2, 4000), (true, 2, 5000), (false, 2, 6000),
    (NULL, 3, 7000), (true, 3, 8000);

-- Test BOOL_AND (all values must be true) and BOOL_OR (any value can be true)
 -- Should be true
SELECT bool_and(b) FROM bool_test WHERE g = 1;

-- Should be false
SELECT bool_and(b) FROM bool_test WHERE g = 2;  

 -- Should be true (NULL ignored)
SELECT bool_and(b) FROM bool_test WHERE g = 3;

 -- Should be true
SELECT bool_or(b) FROM bool_test WHERE g = 1; 

-- Should be true
SELECT bool_or(b) FROM bool_test WHERE g = 2;

-- Should be true
SELECT bool_or(b) FROM bool_test WHERE g = 3;

-- Test with GROUP BY
SELECT g, bool_and(b), bool_or(b) FROM bool_test GROUP BY g ORDER BY g;

-- Test all true values
CREATE TABLE all_true(b BOOLEAN, ts TIMESTAMP TIME INDEX);

INSERT INTO all_true VALUES (true, 1000), (true, 2000), (true, 3000);

SELECT bool_and(b), bool_or(b) FROM all_true;

-- Test all false values  
CREATE TABLE all_false(b BOOLEAN, ts TIMESTAMP TIME INDEX);

INSERT INTO all_false VALUES (false, 1000), (false, 2000), (false, 3000);

SELECT bool_and(b), bool_or(b) FROM all_false;

-- Test all NULL values
CREATE TABLE all_null(b BOOLEAN, ts TIMESTAMP TIME INDEX);

INSERT INTO all_null VALUES (NULL, 1000), (NULL, 2000), (NULL, 3000);

SELECT bool_and(b), bool_or(b) FROM all_null;

-- Test empty result
SELECT bool_and(b), bool_or(b) FROM bool_test WHERE g > 100;

-- Test with integer expressions (converted to boolean)
CREATE TABLE int_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO int_test VALUES (0, 1000), (1, 2000), (2, 3000), (NULL, 4000);

SELECT bool_and(i > 0), bool_or(i > 1) FROM int_test;

-- cleanup
DROP TABLE bool_test;

DROP TABLE all_true;

DROP TABLE all_false;

DROP TABLE all_null;

DROP TABLE int_test;