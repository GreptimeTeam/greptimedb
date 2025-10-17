-- String TRIM and PAD function tests

-- TRIM functions
SELECT TRIM('  hello world  ');

SELECT LTRIM('  hello world  ');

SELECT RTRIM('  hello world  ');

-- TRIM with specific characters
SELECT TRIM('x' FROM 'xxxhello worldxxx');

SELECT LTRIM('hello world', 'hel');

SELECT RTRIM('hello world', 'dlr');

-- PAD functions
SELECT LPAD('hello', 10, '*');

SELECT RPAD('hello', 10, '*');

-- Truncate
SELECT LPAD('hello', 3, '*');

-- Truncate  
SELECT RPAD('hello', 3, '*');

-- PAD with multi-character padding
SELECT LPAD('test', 10, 'ab');

SELECT RPAD('test', 10, 'xy');

-- Test with table data
CREATE TABLE trim_pad_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO trim_pad_test VALUES
    ('  hello  ', 1000),
    ('world   ', 2000),
    ('   test', 3000),
    ('no-spaces', 4000),
    ('', 5000),
    (NULL, 6000);

-- Apply TRIM functions to table data
SELECT s, TRIM(s), LTRIM(s), RTRIM(s) FROM trim_pad_test ORDER BY ts;

-- Apply PAD functions
SELECT s, LPAD(TRIM(s), 15, '-'), RPAD(TRIM(s), 15, '+') FROM trim_pad_test WHERE s IS NOT NULL ORDER BY ts;

-- Test with Unicode characters
SELECT TRIM('  中文测试  ');

SELECT LPAD('🚀', 10, '★');

SELECT RPAD('café', 8, '•');

-- Edge cases
SELECT TRIM('');

SELECT TRIM(NULL);
SELECT LPAD('', 5, '*');

SELECT RPAD('', 5, '*');

SELECT LPAD('test', 0, '*');

SELECT RPAD('test', 0, '*');

-- TRIM with various whitespace characters
SELECT TRIM('\t\nhello\r\n\t');

SELECT LTRIM('\t\nhello world');

SELECT RTRIM('hello world\r\n');

-- Custom TRIM characters
CREATE TABLE custom_trim(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO custom_trim VALUES
    ('***hello***', 1000),
    ('---world---', 2000),
    ('abcTESTabc', 3000);

SELECT s, TRIM('*' FROM s), TRIM('-' FROM s), TRIM('abc' FROM s) FROM custom_trim ORDER BY ts;

DROP TABLE trim_pad_test;

DROP TABLE custom_trim;
