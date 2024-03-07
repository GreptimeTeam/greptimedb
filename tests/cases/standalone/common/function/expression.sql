CREATE TABLE t(a INTEGER, ts timestamp time index);

INSERT INTO t VALUES (1, 1), (null, 2), (3, 3);

SELECT ISNULL(a) from t;

SELECT ISNULL(null);

SELECT ISNULL(1);

SELECT ISNULL(-1);

SELECT ISNULL(1.0);

SELECT ISNULL(true);

SELECT ISNULL('string');

DROP TABLE t;
