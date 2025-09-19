-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_aggregate_types.test
-- Test MIN/MAX aggregate functions

-- Test with strings
CREATE TABLE strings(s STRING, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES ('hello', 0, 1000), ('world', 1, 2000), (NULL, 0, 3000), ('r', 1, 4000);

-- simple aggregates only
SELECT COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings;

SELECT COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings WHERE s IS NULL;

-- grouped aggregates
SELECT g, COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings GROUP BY g ORDER BY g;

-- empty group
SELECT g, COUNT(*), COUNT(s), MIN(s), MAX(s) FROM strings WHERE s IS NULL OR s <> 'hello' GROUP BY g ORDER BY g;

-- Test with integers
CREATE TABLE integers(i INTEGER, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 0, 1000), (5, 1, 2000), (NULL, 0, 3000), (3, 1, 4000), (2, 0, 5000);

SELECT MIN(i), MAX(i) FROM integers;

SELECT g, MIN(i), MAX(i) FROM integers GROUP BY g ORDER BY g;

-- Test with doubles
CREATE TABLE doubles(d DOUBLE, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES (1.5, 0, 1000), (5.5, 1, 2000), (NULL, 0, 3000), (3.5, 1, 4000), (2.5, 0, 5000);

SELECT MIN(d), MAX(d) FROM doubles;

SELECT g, MIN(d), MAX(d) FROM doubles GROUP BY g ORDER BY g;

-- Test with booleans
CREATE TABLE booleans(b BOOLEAN, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO booleans VALUES (false, 0, 1000), (true, 1, 2000), (NULL, 0, 3000), (false, 1, 4000);

SELECT COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans;

SELECT COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans WHERE b IS NULL;

SELECT g, COUNT(*), COUNT(b), MIN(b), MAX(b) FROM booleans GROUP BY g ORDER BY g;

-- cleanup
DROP TABLE strings;

DROP TABLE integers;

DROP TABLE doubles;

DROP TABLE booleans;
