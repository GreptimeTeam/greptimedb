-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_any_value.test
-- Test the ANY_VALUE function

-- Test with NULL values
CREATE TABLE tbl(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO tbl VALUES (NULL, 1000), (2, 2000), (3, 3000);

-- Note: first_value returns first value including NULL, while ANY_VALUE skips NULLs
-- Using first_value as closest available alternative
SELECT first_value(i) AS a FROM tbl;

DROP TABLE tbl;

-- Test with integers
CREATE TABLE five(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO five VALUES (1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000);

SELECT first_value(i) FROM five;

SELECT i % 3 AS g, first_value(i) FROM five GROUP BY g ORDER BY g;

-- Test with ORDER BY
SELECT first_value(i ORDER BY i DESC) FROM five;

SELECT i % 3 AS g, first_value(i ORDER BY i DESC) FROM five GROUP BY g ORDER BY g;

-- Test with doubles
CREATE TABLE doubles(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES (1.5, 1000), (2.5, 2000), (3.5, 3000), (4.5, 4000), (5.5, 5000);

SELECT first_value(d) FROM doubles;

SELECT CAST(d AS INTEGER) % 3 AS g, first_value(d) FROM doubles GROUP BY g ORDER BY g;

SELECT first_value(d ORDER BY d DESC) FROM doubles;

-- Test with strings
CREATE TABLE strings(s VARCHAR, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES ('apple', 1, 1000), ('banana', 1, 2000), ('cherry', 2, 3000), ('date', 2, 4000), ('elderberry', 3, 5000);

SELECT first_value(s) FROM strings;

SELECT g, first_value(s) FROM strings GROUP BY g ORDER BY g;

SELECT first_value(s ORDER BY s DESC) FROM strings;

SELECT g, first_value(s ORDER BY s DESC) FROM strings GROUP BY g ORDER BY g;

-- Test with mixed NULL values
INSERT INTO strings VALUES (NULL, 1, 6000), ('fig', NULL, 7000), (NULL, NULL, 8000);

SELECT first_value(s) FROM strings;

SELECT g, first_value(s) FROM strings WHERE g IS NOT NULL GROUP BY g ORDER BY g;

-- cleanup
DROP TABLE five;

DROP TABLE doubles;

DROP TABLE strings;
