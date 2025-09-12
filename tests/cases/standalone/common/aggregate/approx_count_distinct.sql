-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_approximate_distinct_count.test
-- Test approx_count_distinct operator

-- Basic tests
SELECT APPROX_DISTINCT(1);

SELECT APPROX_DISTINCT(NULL);

SELECT APPROX_DISTINCT('hello');

-- Test with range data
SELECT APPROX_DISTINCT(10), APPROX_DISTINCT('hello') FROM numbers LIMIT 100;

SELECT APPROX_DISTINCT(number) FROM numbers LIMIT 100 WHERE 1 = 0;

-- Test with different data types
CREATE TABLE dates_test(t DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO dates_test VALUES
    ('2008-01-01', 1000), (NULL, 2000), ('2007-01-01', 3000),
    ('2008-02-01', 4000), ('2008-01-02', 5000), ('2008-01-01', 6000),
    ('2008-01-01', 7000), ('2008-01-01', 8000);

SELECT APPROX_DISTINCT(t) FROM dates_test;

DROP TABLE dates_test;

CREATE TABLE names_test(t VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO names_test VALUES
    ('Pedro', 1000), (NULL, 2000), ('Pedro', 3000), ('Pedro', 4000),
    ('Mark', 5000), ('Mark', 6000), ('Mark', 7000),
    ('Hannes-Muehleisen', 8000), ('Hannes-Muehleisen', 9000);

SELECT APPROX_DISTINCT(t) FROM names_test;

DROP TABLE names_test;

-- Test with large dataset
CREATE TABLE large_test(a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO large_test SELECT number, number % 10, number * 1000 FROM numbers LIMIT 2000;

SELECT APPROX_DISTINCT(a), APPROX_DISTINCT(b) FROM large_test;

-- Test with groups
SELECT b, APPROX_DISTINCT(a) FROM large_test GROUP BY b ORDER BY b;

DROP TABLE large_test;