CREATE TABLE t(a INTEGER, ts timestamp time index);

INSERT INTO t VALUES (1, 1), (null, 2), (3, 3);

SELECT ISNULL(a) from t;

SELECT ISNULL(null);

SELECT ISNULL(1);

SELECT ISNULL(-1);

SELECT ISNULL(1.0);

SELECT ISNULL(true);

SELECT ISNULL('string');

SELECT FIRST_VALUE(1);

SELECT FIRST_VALUE('a');

SELECT LAST_VALUE(1);

SELECT LAST_VALUE('a');

-- MySQL-compatible IF function tests
SELECT IF(true, 'yes', 'no');

SELECT IF(false, 'yes', 'no');

SELECT IF(NULL, 'yes', 'no');

SELECT IF(1, 'yes', 'no');

SELECT IF(0, 'yes', 'no');

SELECT IF(-1, 'yes', 'no');

SELECT IF(1.5, 'yes', 'no');

SELECT IF(0.0, 'yes', 'no');

-- Test with table column
SELECT IF(a > 1, 'greater', 'not greater') FROM t;

-- Test numeric return types
SELECT IF(true, 100, 200);

SELECT IF(false, 100, 200);

-- Test with IFNULL (should already work via DataFusion)
SELECT IFNULL(NULL, 'default');

SELECT IFNULL('value', 'default');

-- Test COALESCE (should already work via DataFusion)
SELECT COALESCE(NULL, NULL, 'third');

SELECT COALESCE('first', 'second');

DROP TABLE t;
