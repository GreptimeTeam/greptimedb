-- String REPLACE function tests

-- Basic REPLACE function
SELECT REPLACE('hello world', 'world', 'universe');

SELECT REPLACE('hello world', 'xyz', 'abc');

SELECT REPLACE('hello hello hello', 'hello', 'hi');

-- REPLACE with empty strings
SELECT REPLACE('hello world', 'world', '');

SELECT REPLACE('hello world', '', 'xyz');

SELECT REPLACE('', 'xyz', 'abc');

-- Case sensitive replacement
SELECT REPLACE('Hello World', 'hello', 'hi');

SELECT REPLACE('Hello World', 'Hello', 'Hi');

-- NULL handling
SELECT REPLACE(NULL, 'world', 'universe');

SELECT REPLACE('hello world', NULL, 'universe');

SELECT REPLACE('hello world', 'world', NULL);

-- Test with table data
CREATE TABLE replace_test(s VARCHAR, old_str VARCHAR, new_str VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO replace_test VALUES
    ('hello world', 'world', 'universe', 1000),
    ('programming language', 'language', 'paradigm', 2000),
    ('test test test', 'test', 'exam', 3000),
    ('no match here', 'xyz', 'abc', 4000);

SELECT s, old_str, new_str, REPLACE(s, old_str, new_str) FROM replace_test ORDER BY ts;

-- Unicode replacement
SELECT REPLACE('hello 世界', '世界', 'world');

SELECT REPLACE('café shop', 'é', 'e');

SELECT REPLACE('🚀 rocket 🚀', '🚀', '✈️');

-- Multiple character replacement
SELECT REPLACE('hello-world-test', '-', '_');

SELECT REPLACE('abc::def::ghi', '::', '-->');

-- Overlapping patterns
SELECT REPLACE('ababab', 'ab', 'xy');

SELECT REPLACE('aaa', 'aa', 'b');

DROP TABLE replace_test;
