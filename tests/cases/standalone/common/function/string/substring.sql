-- Migrated from DuckDB test: test/sql/function/string/test_substring.test
-- Substring function tests

CREATE TABLE strings(s VARCHAR, "off" INTEGER, length INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES
    ('hello', 1, 2, 1000),
    ('world', 2, 3, 2000),
    ('b', 1, 1, 3000),
    (NULL, 2, 2, 4000);

-- Test zero length
SELECT SUBSTRING('🦆ab', 1, 0), SUBSTRING('abc', 1, 0);

-- Normal substring with constant offset/length
SELECT SUBSTRING(s, 1, 2) FROM strings ORDER BY ts;

-- Substring out of range
SELECT SUBSTRING(s, 2, 2) FROM strings ORDER BY ts;

-- Variable length offset/length
SELECT SUBSTRING(s, "off", "length") FROM strings ORDER BY ts;

SELECT SUBSTRING(s, "off", 2) FROM strings ORDER BY ts;

SELECT SUBSTRING(s, 1, length) FROM strings ORDER BY ts;

SELECT SUBSTRING('hello', "off", length) FROM strings ORDER BY ts;

-- Test with NULL values
SELECT SUBSTRING(NULL,  "off", length) FROM strings ORDER BY ts;

SELECT SUBSTRING(s, NULL, length) FROM strings ORDER BY ts;

SELECT SUBSTRING(s,  "off", NULL) FROM strings ORDER BY ts;

-- Test negative offsets
SELECT SUBSTRING('hello', -1, 3);
SELECT SUBSTRING('hello', 0, 3);

-- Test with Unicode characters
CREATE TABLE unicode_strings(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO unicode_strings VALUES
    ('Hello 世界', 1000),
    ('🦆🦀🐧', 2000),
    ('café', 3000);

SELECT s, SUBSTRING(s, 1, 5), SUBSTRING(s, 7, 2) FROM unicode_strings ORDER BY ts;

DROP TABLE strings;

DROP TABLE unicode_strings;
