-- String length function tests

-- LENGTH function
SELECT LENGTH('hello');

SELECT LENGTH('');

SELECT LENGTH(NULL);

SELECT LENGTH('hello world');

-- CHAR_LENGTH (character length)
SELECT CHAR_LENGTH('hello');

SELECT CHAR_LENGTH('');

SELECT CHAR_LENGTH(NULL);

-- CHARACTER_LENGTH (alias for CHAR_LENGTH)
SELECT CHARACTER_LENGTH('hello world');

-- Unicode character length
SELECT LENGTH('世界') AS a, CHAR_LENGTH('世界') AS b;

SELECT LENGTH('🚀🌟') AS a, CHAR_LENGTH('🚀🌟') AS b;

SELECT LENGTH('café') AS a, CHAR_LENGTH('café') AS b;

-- Test with table data
CREATE TABLE length_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO length_test VALUES
    ('hello', 1000),
    ('world!', 2000),
    ('', 3000),
    ('中文测试', 4000),
    ('🚀🎉🌟', 5000),
    (NULL, 6000);

SELECT s, LENGTH(s) AS a, CHAR_LENGTH(s) AS b FROM length_test ORDER BY ts;

-- BIT_LENGTH (length in bits)
SELECT BIT_LENGTH('hello');

SELECT BIT_LENGTH('');

SELECT BIT_LENGTH('世界');

-- OCTET_LENGTH (length in bytes)
SELECT OCTET_LENGTH('hello');

SELECT OCTET_LENGTH('');

SELECT OCTET_LENGTH('世界');

SELECT OCTET_LENGTH('🚀');

DROP TABLE length_test;
