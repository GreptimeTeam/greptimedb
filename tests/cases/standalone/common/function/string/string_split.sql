-- Migrated from DuckDB test: test/sql/function/string/test_string_split.test
-- String split function tests

-- Test basic string_split functionality
SELECT string_to_array(NULL, NULL);

SELECT string_to_array('hello world', ' ');

SELECT string_to_array(NULL, ' ');

SELECT string_to_array('a b c', NULL);

SELECT string_to_array('a b c', ' ');

-- Test with table data
CREATE TABLE split_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO split_test VALUES
    ('hello,world,test', 1000),
    ('a|b|c|d', 2000),
    ('no-separator', 3000),
    ('', 4000),
    (NULL, 5000);

-- Test splitting with different separators
SELECT s, string_to_array(s, ',') FROM split_test ORDER BY ts;

SELECT s, string_to_array(s, '|') FROM split_test ORDER BY ts;

SELECT s, string_to_array(s, '-') FROM split_test ORDER BY ts;

-- Test splitting with multi-character separator
CREATE TABLE multi_sep_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO multi_sep_test VALUES
    ('hello::world::test', 1000),
    ('a---b---c', 2000),
    ('single', 3000);

SELECT s, string_to_array(s, '::') FROM multi_sep_test ORDER BY ts;

SELECT s, string_to_array(s, '---') FROM multi_sep_test ORDER BY ts;

-- Test with Unicode separators
CREATE TABLE unicode_split_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO unicode_split_test VALUES
    ('hello世world世test', 1000),
    ('a🦆b🦆c', 2000);

SELECT s, string_to_array(s, '世') FROM unicode_split_test ORDER BY ts;

SELECT s, string_to_array(s, '🦆') FROM unicode_split_test ORDER BY ts;

-- Test edge cases
-- Empty string
SELECT string_to_array('', ',');

-- Empty separator
SELECT string_to_array('hello', '');

-- Multiple consecutive separators
SELECT string_to_array(',,hello,,world,,', ',');

-- Trailing separator
SELECT string_to_array('hello,', ',');

-- Leading separator
SELECT string_to_array(',hello', ',');

DROP TABLE split_test;

DROP TABLE multi_sep_test;

DROP TABLE unicode_split_test;
