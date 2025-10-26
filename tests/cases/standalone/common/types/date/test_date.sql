-- Migrated from DuckDB test: test/sql/types/date/test_date.test
-- Test basic DATE functionality

-- Create and insert into table
CREATE TABLE dates(i DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO dates VALUES ('1993-08-14', 1000), (NULL, 2000);

-- Check that we can select dates
SELECT * FROM dates ORDER BY ts;

-- extract function
SELECT extract(year FROM i) FROM dates ORDER BY ts;

-- Check that we can convert dates to string
SELECT CAST(i AS VARCHAR) FROM dates ORDER BY ts;

-- Check that we can add days to a date
SELECT i + INTERVAL '5 days' FROM dates ORDER BY ts;

-- Check that we can subtract days from a date
SELECT i - INTERVAL '5 days' FROM dates ORDER BY ts;

-- Test date subtraction resulting in interval
SELECT i - DATE '1993-08-14' FROM dates ORDER BY ts;

-- Test various date formats
CREATE TABLE date_formats(d DATE, ts TIMESTAMP TIME INDEX);

INSERT INTO date_formats VALUES
    ('2021-03-01', 1000),
    ('2021-12-31', 2000),
    ('2000-01-01', 3000),
    ('1970-01-01', 4000);

SELECT d,  extract(year FROM d), extract(month FROM d), extract(day FROM d) FROM date_formats ORDER BY d;

-- Test date comparison
SELECT d FROM date_formats WHERE d > '2000-01-01' ORDER BY d;

SELECT d FROM date_formats WHERE d BETWEEN '2000-01-01' AND '2021-06-01' ORDER BY d;

-- Test NULL handling
INSERT INTO date_formats VALUES (NULL, 5000);

SELECT COUNT(*), COUNT(d) FROM date_formats;

DROP TABLE dates;

DROP TABLE date_formats;
