-- Migrated from DuckDB test: test/sql/aggregate/aggregates/test_string_agg.test
-- Test STRING_AGG operator

-- test string aggregation on scalar values
SELECT STRING_AGG('a',',');

-- test string aggregation on scalar values with NULL
SELECT STRING_AGG('a',','), STRING_AGG(NULL,','), STRING_AGG('a', NULL), STRING_AGG(NULL,NULL);

-- test string aggregation on a set of values
CREATE TABLE strings(g INTEGER, x VARCHAR, y VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES
    (1,'a','/', 1000), (1,'b','-', 2000),
    (2,'i','/', 3000), (2,NULL,'-', 4000), (2,'j','+', 5000),
    (3,'p','/', 6000),
    (4,'x','/', 7000), (4,'y','-', 8000), (4,'z','+', 9000);

SELECT g, STRING_AGG(x,'|') FROM strings GROUP BY g ORDER BY g;

-- test agg on empty set
SELECT STRING_AGG(x,',') FROM strings WHERE g > 100;

-- string_agg can be used instead of group_concat
SELECT string_agg('a', ',');

SELECT string_agg('a', ',');

SELECT g, string_agg(x, ',') FROM strings GROUP BY g ORDER BY g;

-- Test ORDER BY
-- Single group
SELECT STRING_AGG(x, '' ORDER BY x ASC), STRING_AGG(x, '|' ORDER BY x ASC) FROM strings;

SELECT STRING_AGG(x, '' ORDER BY x DESC), STRING_AGG(x,'|' ORDER BY x DESC) FROM strings;

-- Grouped with ORDER BY
SELECT g, STRING_AGG(x, '' ORDER BY x ASC), STRING_AGG(x, '|' ORDER BY x ASC) FROM strings GROUP BY g ORDER BY g;

SELECT g, STRING_AGG(x, '' ORDER BY x DESC), STRING_AGG(x,'|' ORDER BY x DESC) FROM strings GROUP BY g ORDER BY g;

-- Test with DISTINCT
SELECT STRING_AGG(DISTINCT x, '' ORDER BY x), STRING_AGG(DISTINCT x, '|' ORDER BY x) FROM strings;

SELECT g, STRING_AGG(DISTINCT x, '' ORDER BY x) FROM strings GROUP BY g ORDER BY g;

-- cleanup
DROP TABLE strings;
