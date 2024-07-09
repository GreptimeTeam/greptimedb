CREATE TABLE t1 (n INT, ts TIMESTAMP TIME INDEX);

INSERT INTO t1 VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);

CREATE VIEW IF NOT EXISTS v1 (a) AS SELECT * FROM t1;

CREATE VIEW IF NOT EXISTS v1 (a) AS SELECT n FROM t1;

SHOW CREATE VIEW v1;

SELECT * FROM v1;

SELECT a FROM v1;

SELECT n FROM v1;

CREATE OR REPLACE VIEW v1 (a, b) AS SELECT n, n+1 FROM t1;

SHOW CREATE VIEW v1;

SELECT * FROM v1;

SELECT * FROM v1 WHERE a > 5;

SELECT * FROM v1 WHERE b > 5;

SELECT a FROM v1;

SELECT b FROM v1;

SELECT a,b FROM v1;

SELECT n FROM v1;

SELECT * FROM v1 WHERE n > 5;

-- test view after altering table t1 --
CREATE OR REPLACE VIEW v1 AS SELECT n, ts FROM t1 LIMIT 5;

SELECT * FROM v1;

ALTER TABLE t1 ADD COLUMN s STRING DEFAULT '';

SELECT * FROM v1;

ALTER TABLE t1 DROP COLUMN n;

-- FIXME(dennis): The result looks weird,
-- Looks like substrait referes to columns only by their relative indices, so that’s name-independent.
-- Limit: skip=0, fetch=5
--  Projection: greptime.public.t1.ts, greptime.public.t1.s
--    MergeScan [is_placeholder=false]
-- Limit: skip=0, fetch=5
--  MergeScan [is_placeholder=false]
-- See https://github.com/apache/datafusion/issues/6489
SELECT * FROM v1;

DROP VIEW v1;

DROP TABLE t1;
