-- String case conversion function tests

-- Basic UPPER and LOWER functions
SELECT UPPER('hello world');

SELECT LOWER('HELLO WORLD');

SELECT UPPER('MiXeD cAsE');

SELECT LOWER('MiXeD cAsE');

-- INITCAP (capitalize first letter of each word)
SELECT INITCAP('hello world');

SELECT INITCAP('HELLO WORLD');

SELECT INITCAP('mIxEd CaSe TeSt');

-- Test with NULL
SELECT UPPER(NULL);

SELECT LOWER(NULL);

SELECT INITCAP(NULL);

-- Test with numbers and special characters
SELECT UPPER('hello123!@#');

SELECT LOWER('HELLO123!@#');

SELECT INITCAP('hello-world_test');

-- Test with table data
CREATE TABLE case_test("name" VARCHAR, city VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO case_test VALUES
    ('john doe', 'new york', 1000),
    ('JANE SMITH', 'LOS ANGELES', 2000),
    ('Bob Wilson', 'Chicago', 3000),
    ('alice johnson', 'BOSTON', 4000);

-- Apply case functions to table data
SELECT "name", UPPER("name"), LOWER("name"), INITCAP("name") FROM case_test ORDER BY ts;

SELECT city, UPPER(city), LOWER(city), INITCAP(city) FROM case_test ORDER BY ts;

-- Combined case operations
SELECT INITCAP(LOWER("name")) as formatted_name FROM case_test ORDER BY ts;

-- Unicode case conversion
SELECT UPPER('café');

SELECT LOWER('CAFÉ');

-- German characters
SELECT UPPER('äöüß');

-- German uppercase
SELECT LOWER('ÄÖÜ');

-- Greek letters
SELECT UPPER('αβγ');

SELECT LOWER('ΑΒΓ');

-- Test with empty string
SELECT UPPER('');

SELECT LOWER('');

SELECT INITCAP('');

-- Test with single characters
SELECT UPPER('a'), UPPER('A'), UPPER('1'), UPPER(' ');

SELECT LOWER('a'), LOWER('A'), LOWER('1'), LOWER(' ');

SELECT INITCAP('a'), INITCAP('A'), INITCAP('1');

-- Complex Unicode examples
CREATE TABLE unicode_case(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO unicode_case VALUES
    ('hello 世界', 1000),
    ('HELLO 世界', 2000),
    ('café à paris', 3000),
    ('CAFÉ À PARIS', 4000);

SELECT s, UPPER(s), LOWER(s), INITCAP(s) FROM unicode_case ORDER BY ts;

DROP TABLE case_test;

DROP TABLE unicode_case;
