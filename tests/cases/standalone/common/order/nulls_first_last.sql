-- Migrated from DuckDB test: test/sql/order/test_nulls_first.test
-- Test NULLS FIRST/NULLS LAST

CREATE TABLE integers(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 1000), (NULL, 2000);

-- Default NULL ordering (usually NULLS LAST in most systems)
SELECT i FROM integers ORDER BY i;

-- Explicit NULLS FIRST
SELECT i FROM integers ORDER BY i NULLS FIRST;

-- Explicit NULLS LAST
SELECT i FROM integers ORDER BY i NULLS LAST;

-- Multiple columns with mixed NULL handling
CREATE TABLE test(i INTEGER, j INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test VALUES (1, 1, 1000), (NULL, 1, 2000), (1, NULL, 3000);

SELECT i, j FROM test ORDER BY i NULLS FIRST, j NULLS LAST;

SELECT i, j FROM test ORDER BY i NULLS FIRST, j NULLS FIRST;

SELECT i, j FROM test ORDER BY i NULLS LAST, j NULLS FIRST;

-- Test with DESC ordering
SELECT i, j FROM test ORDER BY i DESC NULLS FIRST, j DESC NULLS LAST;

SELECT i, j FROM test ORDER BY i DESC NULLS LAST, j DESC NULLS FIRST;

-- Test with strings
CREATE TABLE strings(s VARCHAR, i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES ('apple', 1, 1000), (NULL, 2, 2000), ('banana', NULL, 3000);

SELECT s, i FROM strings ORDER BY s NULLS FIRST, i NULLS LAST;

SELECT s, i FROM strings ORDER BY s NULLS LAST, i NULLS FIRST;

DROP TABLE integers;

DROP TABLE test;

DROP TABLE strings;
