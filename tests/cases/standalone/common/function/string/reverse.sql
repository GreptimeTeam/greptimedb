-- String REVERSE function tests

-- Basic REVERSE function
SELECT REVERSE('hello');

SELECT REVERSE('world');

SELECT REVERSE('');

SELECT REVERSE(NULL);

-- REVERSE with numbers and special characters
SELECT REVERSE('12345');

SELECT REVERSE('hello!');

SELECT REVERSE('a!@#$%b');

-- REVERSE with palindromes
SELECT REVERSE('radar');

SELECT REVERSE('madam');

SELECT REVERSE('racecar');

-- Test with table data
CREATE TABLE reverse_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO reverse_test VALUES
    ('hello', 1000),
    ('world', 2000),
    ('12345', 3000),
    ('radar', 4000),
    ('', 5000),
    (NULL, 6000);

SELECT s, REVERSE(s) FROM reverse_test ORDER BY ts;

-- Unicode REVERSE
SELECT REVERSE('世界');

SELECT REVERSE('café');

SELECT REVERSE('🚀🌟');

-- REVERSE with spaces
SELECT REVERSE('hello world');

SELECT REVERSE('  spaces  ');

-- Combining REVERSE with other functions
SELECT UPPER(REVERSE('hello'));

SELECT REVERSE(UPPER('hello'));

SELECT LENGTH(REVERSE('hello world'));

-- Double REVERSE (should return original)
SELECT REVERSE(REVERSE('hello world'));

SELECT REVERSE(REVERSE('中文测试'));

DROP TABLE reverse_test;
