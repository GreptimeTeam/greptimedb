-- String concatenation function tests
-- Test CONCAT function

-- Basic concatenation
SELECT CONCAT('hello', 'world');

SELECT CONCAT('hello', ' ', 'world');

SELECT CONCAT('a', 'b', 'c', 'd');

-- Concatenation with NULL values
SELECT CONCAT('hello', NULL);

SELECT CONCAT(NULL, 'world');

SELECT CONCAT(NULL, NULL);

-- Concatenation with numbers (should convert to string)
SELECT CONCAT('value: ', 42);

SELECT CONCAT(1, 2, 3);

-- Test with table data
CREATE TABLE concat_test(first_name VARCHAR, last_name VARCHAR, age INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO concat_test VALUES
    ('John', 'Doe', 30, 1000),
    ('Jane', 'Smith', 25, 2000),
    ('Bob', NULL, 35, 3000),
    (NULL, 'Wilson', 40, 4000);

-- Concatenate table columns
SELECT CONCAT(first_name, ' ', last_name) as full_name FROM concat_test ORDER BY ts;

SELECT CONCAT(first_name, ' is ', age, ' years old') FROM concat_test ORDER BY ts;

-- Test CONCAT_WS (concat with separator)
SELECT CONCAT_WS(' ', first_name, last_name) as full_name FROM concat_test ORDER BY ts;

SELECT CONCAT_WS('-', first_name, last_name, age) FROM concat_test ORDER BY ts;

SELECT CONCAT_WS(',', 'a', 'b', 'c', 'd');

-- CONCAT_WS with NULL values (should skip NULLs)
SELECT CONCAT_WS(' ', 'hello', NULL, 'world');

SELECT CONCAT_WS('|', first_name, last_name) FROM concat_test ORDER BY ts;

-- Test pipe operator ||
SELECT 'hello' || 'world';

SELECT 'hello' || ' ' || 'world';

SELECT first_name || ' ' || last_name FROM concat_test WHERE first_name IS NOT NULL AND last_name IS NOT NULL ORDER BY ts;

-- Unicode concatenation
SELECT CONCAT('Hello ', '世界');

SELECT CONCAT('🚀', ' ', '🌟');

SELECT '中文' || '🐄';

DROP TABLE concat_test;
