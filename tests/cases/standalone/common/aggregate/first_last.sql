-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_last.test
-- Test FIRST and LAST aggregate functions

-- Test with integers
CREATE TABLE five(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO five VALUES (1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000);

SELECT last_value(i) FROM five;

SELECT first_value(i) FROM five;

SELECT i % 3 AS g, last_value(i) FROM five GROUP BY g ORDER BY g;

SELECT i % 3 AS g, first_value(i) FROM five GROUP BY g ORDER BY g;

-- Test with ORDER BY
SELECT last_value(i ORDER BY i DESC) FROM five;

SELECT first_value(i ORDER BY i DESC) FROM five;

SELECT i % 3 AS g, last_value(i ORDER BY i DESC) FROM five GROUP BY g ORDER BY g;

SELECT i % 3 AS g, first_value(i ORDER BY i DESC) FROM five GROUP BY g ORDER BY g;

-- Test with strings
CREATE TABLE strings(s VARCHAR, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES ('apple', 1, 1000), ('banana', 1, 2000), ('cherry', 2, 3000), ('date', 2, 4000), ('elderberry', 3, 5000);

SELECT last_value(s) FROM strings;

SELECT first_value(s) FROM strings;

SELECT g, last_value(s) FROM strings GROUP BY g ORDER BY g;

SELECT g, first_value(s) FROM strings GROUP BY g ORDER BY g;

-- Test with ORDER BY on strings
SELECT last_value(s ORDER BY s) FROM strings;

SELECT first_value(s ORDER BY s) FROM strings;

SELECT g, last_value(s ORDER BY s) FROM strings GROUP BY g ORDER BY g;

SELECT g, first_value(s ORDER BY s) FROM strings GROUP BY g ORDER BY g;

-- Test with NULL values
INSERT INTO strings VALUES (NULL, 1, 6000), ('fig', NULL, 7000);

SELECT last_value(s) FROM strings;

SELECT first_value(s) FROM strings;

SELECT g, last_value(s) FROM strings WHERE g IS NOT NULL GROUP BY g ORDER BY g;

-- Test with dates
CREATE TABLE dates(d DATE, i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO dates VALUES ('2021-08-20', 1, 1000), ('2021-08-21', 2, 2000), ('2021-08-22', 3, 3000), ('2021-08-23', 4, 4000), ('2021-08-24', 5, 5000);

SELECT last_value(d) FROM dates;

SELECT first_value(d) FROM dates;

SELECT i % 3 AS g, last_value(d) FROM dates GROUP BY g ORDER BY g;

SELECT i % 3 AS g, first_value(d) FROM dates GROUP BY g ORDER BY g;

-- cleanup
DROP TABLE five;

DROP TABLE strings;

DROP TABLE dates;
