-- Migrated from DuckDB test: test/sql/types/string/test_big_strings.test
-- Test handling of large strings

-- Test large string creation and manipulation
CREATE TABLE big_strings("id" INTEGER, s VARCHAR, ts TIMESTAMP TIME INDEX);

-- Insert strings of various sizes
INSERT INTO big_strings VALUES
    (1, REPEAT('a', 100), 1000),
    (2, REPEAT('Hello World! ', 50), 2000),
    (3, REPEAT('Unicode 世界 ', 100), 3000),
    (4, REPEAT('x', 1000), 4000);

-- Test length of big strings
SELECT "id", LENGTH(s) FROM big_strings ORDER BY "id";

-- Test substring operations on big strings
SELECT "id", SUBSTRING(s, 1, 20) FROM big_strings ORDER BY "id";

SELECT "id", RIGHT(s, 10) FROM big_strings ORDER BY "id";

-- Test concatenation with big strings
SELECT "id", LENGTH(s || s) FROM big_strings WHERE "id" = 1;

-- Test pattern matching on big strings
SELECT "id", s LIKE '%World%' FROM big_strings ORDER BY "id";

-- Test comparison with big strings
SELECT COUNT(*) FROM big_strings WHERE s = REPEAT('a', 100);

-- Test UPPER/LOWER on big strings
SELECT "id", LENGTH(UPPER(s)) FROM big_strings WHERE "id" <= 2 ORDER BY "id";

-- Test trimming big strings
CREATE TABLE padded_strings(s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO padded_strings VALUES (CONCAT('   ', REPEAT('test', 100), '   '), 1000);

SELECT LENGTH(s), LENGTH(TRIM(s)) FROM padded_strings;

DROP TABLE big_strings;

DROP TABLE padded_strings;
