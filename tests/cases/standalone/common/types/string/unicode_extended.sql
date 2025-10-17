-- Migrated from DuckDB test: test/sql/types/string/test_unicode.test
-- Test Unicode string handling

-- Test basic Unicode strings
CREATE TABLE unicode_test(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO unicode_test VALUES
    ('Hello 世界', 1000),
    ('Ññññ', 2000),
    ('🚀🎉🌟', 3000),
    ('Здравствуй мир', 4000),
    ('مرحبا بالعالم', 5000),
    ('こんにちは世界', 6000);

-- Test basic selection
SELECT s FROM unicode_test ORDER BY ts;

-- Test length function with Unicode
SELECT s, LENGTH(s) AS a, CHAR_LENGTH(s) AS b FROM unicode_test ORDER BY ts;

-- Test substring with Unicode
SELECT s, SUBSTRING(s, 1, 5) FROM unicode_test ORDER BY ts;

-- Test UPPER/LOWER with Unicode
SELECT s, UPPER(s), LOWER(s) FROM unicode_test WHERE s = 'Hello 世界';

-- Test comparison with Unicode
SELECT COUNT(*) FROM unicode_test WHERE s LIKE '%世界%';

SELECT COUNT(*) FROM unicode_test WHERE s LIKE '%🚀%';

-- Test concatenation with Unicode
SELECT CONCAT(s, ' - test') FROM unicode_test ORDER BY ts;

DROP TABLE unicode_test;
