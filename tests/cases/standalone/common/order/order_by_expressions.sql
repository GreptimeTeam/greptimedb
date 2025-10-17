-- Migrated from DuckDB test: test/sql/order/test_order_by_expressions.test
-- Test ORDER BY with expressions

CREATE TABLE test(a INTEGER, b INTEGER, s VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES
    (1, 10, 'apple', 1000),
    (2, 20, 'banana', 2000),
    (3, 15, 'cherry', 3000),
    (4, 25, 'date', 4000);

-- ORDER BY with arithmetic expressions
SELECT a, b, a + b as sum FROM test ORDER BY a + b;

SELECT a, b, a * b as product FROM test ORDER BY a * b DESC;

-- ORDER BY with string functions
SELECT s, LENGTH(s) as len FROM test ORDER BY LENGTH(s);

SELECT s, UPPER(s) as upper_s FROM test ORDER BY UPPER(s);

-- ORDER BY with CASE expressions
SELECT a, b,
    CASE
        WHEN a % 2 = 0 THEN 'even'
        ELSE 'odd'
    END as parity
FROM test
ORDER BY
    CASE
        WHEN a % 2 = 0 THEN 1
        ELSE 2
    END, a;

-- ORDER BY with conditional expressions
SELECT a, b FROM test ORDER BY GREATEST(a, b) DESC;

SELECT a, b FROM test ORDER BY LEAST(a, b);

-- ORDER BY with NULL-related expressions
INSERT INTO test VALUES (NULL, NULL, NULL, 5000);

SELECT a, b, COALESCE(a, 999) as a_or_999
FROM test
ORDER BY COALESCE(a, 999);

-- ORDER BY with subqueries in expressions
SELECT a, b,
    a - (SELECT MIN(a) FROM test WHERE a IS NOT NULL) as diff_from_min
FROM test
WHERE a IS NOT NULL
ORDER BY a - (SELECT MIN(a) FROM test WHERE a IS NOT NULL);

DROP TABLE test;
