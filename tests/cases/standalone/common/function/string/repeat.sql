-- String REPEAT function tests

-- Basic REPEAT function
SELECT REPEAT('hello', 3);

SELECT REPEAT('a', 5);

SELECT REPEAT('', 3);

SELECT REPEAT('test', 0);

SELECT REPEAT('test', 1);

-- REPEAT with NULL values
SELECT REPEAT(NULL, 3);

SELECT REPEAT('hello', NULL);

-- REPEAT with negative numbers
SELECT REPEAT('hello', -1);

-- REPEAT with special characters
SELECT REPEAT('*', 10);

SELECT REPEAT('-=', 5);

SELECT REPEAT('!@#', 3);

-- Test with table data
CREATE TABLE repeat_test(s VARCHAR, n INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO repeat_test VALUES
    ('hello', 2, 1000),
    ('*', 5, 2000),
    ('test', 0, 3000),
    ('a', 10, 4000),
    (NULL, 3, 5000),
    ('hi', NULL, 6000);

SELECT s, n, REPEAT(s, n) FROM repeat_test ORDER BY ts;

-- Unicode REPEAT
SELECT REPEAT('世', 3);

SELECT REPEAT('🚀', 5);

SELECT REPEAT('café', 2);

-- REPEAT with spaces and formatting
SELECT REPEAT(' ', 10);

SELECT REPEAT('\t', 3);

SELECT CONCAT('Start', REPEAT('-', 10), 'End');

-- Large REPEAT operations
SELECT LENGTH(REPEAT('a', 100));

SELECT LENGTH(REPEAT('ab', 50));

-- Combining REPEAT with other functions
SELECT UPPER(REPEAT('hello', 3));

SELECT REPEAT(UPPER('hello'), 2);

SELECT REVERSE(REPEAT('abc', 3));

DROP TABLE repeat_test;
