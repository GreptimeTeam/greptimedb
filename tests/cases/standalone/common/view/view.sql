-- From: https://github.com/duckdb/duckdb/blob/main/test/sql/catalog/view/test_view.test --

CREATE DATABASE schema_for_view_test;

USE schema_for_view_test;

CREATE TABLE t1(i TIMESTAMP TIME INDEX);

INSERT INTO t1 VALUES (41), (42), (43);

CREATE VIEW v1 AS SELECT
	i AS j
FROM t1 WHERE i < 43;

SELECT * FROM v1;

-- CREATE VIEW v1 AS SELECT 'whatever'; --

SELECT j FROM v1 WHERE j > 41;


-- FIXME(dennis):: name alias in view, not supported yet --
--SELECT x FROM v1 t1(x) WHERE x > 41 --

-- FIXME(dennis): DROP VIEW not supported yet--
-- DROP VIEW v1 --

-- SELECT j FROM v1 WHERE j > 41 --

-- CREATE VIEW v1 AS SELECT 'whatever'; --

-- SELECT * FROM v1; --


-- CREATE OR REPLACE VIEW v1 AS SELECT 42; --

-- SELECT * FROM v1; --

INSERT INTO v1 VALUES (1);

CREATE VIEW v1 AS SELECT * FROM dontexist;

USE public;

DROP DATABASE schema_for_view_test;
