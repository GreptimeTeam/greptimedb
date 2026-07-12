-- Migrated from DuckDB test: test/sql/types/null/test_null.test
-- Test NULL value handling across different contexts

-- Test NULL in basic operations
CREATE TABLE null_test(i INTEGER, s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO null_test VALUES
    (1, 'hello', 1000),
    (NULL, 'world', 2000),
    (3, NULL, 3000),
    (NULL, NULL, 4000);

-- Test NULL comparisons
SELECT i, s FROM null_test WHERE i IS NULL ORDER BY ts;
SELECT i, s FROM null_test WHERE i IS NOT NULL ORDER BY ts;
SELECT i, s FROM null_test WHERE s IS NULL ORDER BY ts;
SELECT i, s FROM null_test WHERE s IS NOT NULL ORDER BY ts;

-- Test NULL in arithmetic
SELECT i, i + 1, i * 2, i - 5 FROM null_test ORDER BY ts;

-- Test NULL in string operations
SELECT s, CONCAT(s, ' test'), UPPER(s), LENGTH(s) FROM null_test ORDER BY ts;

-- Test NULL with COALESCE
SELECT i, s, COALESCE(i, -1), COALESCE(s, 'missing') FROM null_test ORDER BY ts;

-- Test NULL in aggregates
SELECT COUNT(*), COUNT(i), COUNT(s) FROM null_test;
SELECT SUM(i), AVG(i), MAX(i), MIN(i) FROM null_test;

-- Test NULL in CASE expressions
SELECT i, s,
    CASE
        WHEN i IS NULL THEN 'no number'
        WHEN i > 2 THEN 'big number'
        ELSE 'small number'
    END as category
FROM null_test ORDER BY ts;

-- Test NULL in GROUP BY
SELECT i, COUNT(*) FROM null_test GROUP BY i ORDER BY i;
SELECT s, COUNT(*) FROM null_test GROUP BY s ORDER BY s;

-- Test NULLIF function
SELECT i, NULLIF(i, 1) FROM null_test ORDER BY ts;
SELECT s, NULLIF(s, 'hello') FROM null_test ORDER BY ts;

DROP TABLE null_test;
