-- Migrated from DuckDB test style: test approximate median
-- Test APPROX_MEDIAN function

-- Test with odd number of values
CREATE TABLE odd_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO odd_test VALUES (1, 1000), (2, 2000), (3, 3000), (4, 4000), (5, 5000);

-- Should return 3 (middle value)
SELECT approx_median(i) FROM odd_test;

-- Test with even number of values
CREATE TABLE even_test(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO even_test VALUES (1, 1000), (2, 2000), (4, 3000), (5, 4000);

-- Should return approximately 3 (average of 2 and 4)
SELECT approx_median(i) FROM even_test;

-- Test with larger dataset
CREATE TABLE large_test(val INTEGER, grp INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO large_test SELECT number, number % 3, number * 1000 FROM numbers LIMIT 1000;

SELECT approx_median(val) FROM large_test;

-- Test with groups
SELECT grp, approx_median(val) FROM large_test GROUP BY grp ORDER BY grp;

-- Test with doubles
CREATE TABLE double_test(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO double_test VALUES
    (1.1, 1000), (2.2, 2000), (3.3, 3000), (4.4, 4000), (5.5, 5000);

SELECT approx_median(d) FROM double_test;

-- Test with NULL values
INSERT INTO double_test VALUES (NULL, 6000);

SELECT approx_median(d) FROM double_test;

-- Test with duplicate values
CREATE TABLE dup_test(val INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO dup_test VALUES
    (1, 1000), (1, 2000), (2, 3000), (2, 4000),
    (3, 5000), (3, 6000), (4, 7000), (4, 8000);

SELECT approx_median(val) FROM dup_test;

-- Compare with exact median
SELECT median(val), approx_median(val) FROM dup_test;

-- Test edge cases
-- empty result
SELECT approx_median(i) FROM odd_test WHERE i > 100;

-- Test single value
SELECT approx_median(i) FROM odd_test WHERE i = 3;

-- Test with negative values
CREATE TABLE neg_test(val INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO neg_test VALUES (-5, 1000), (-2, 2000), (0, 3000), (3, 4000), (7, 5000);

SELECT approx_median(val) FROM neg_test;

-- cleanup
DROP TABLE odd_test;

DROP TABLE even_test;

DROP TABLE large_test;

DROP TABLE double_test;

DROP TABLE dup_test;

DROP TABLE neg_test;
