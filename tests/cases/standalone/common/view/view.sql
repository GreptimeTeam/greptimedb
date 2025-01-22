-- From: https://github.com/duckdb/duckdb/blob/main/test/sql/catalog/view/test_view.test --
CREATE TABLE t1(i TIMESTAMP TIME INDEX);

INSERT INTO t1 VALUES (41), (42), (43);

CREATE VIEW v1 AS SELECT
	i AS j
FROM t1 WHERE i < 43;

SELECT * FROM v1;

SELECT j FROM v1 WHERE j > 41;

SELECT x FROM v1 t1(x) WHERE x > 41;

INSERT INTO v1 VALUES (1);

-- FIXME(dennis): DROP VIEW not supported yet--
DROP VIEW v1;

-- substrait can't process such query currently
-- CREATE VIEW v1 AS SELECT 'whatever';
-- SELECT * FROM v1;

-- substrait can't process such query currently
-- CREATE OR REPLACE VIEW v1 AS SELECT 42;
-- SELECT * FROM v1;


CREATE VIEW v1 AS SELECT * FROM dontexist;

SHOW VIEWS;

DROP VIEW v1;

SELECT * FROM v1;

--- view not exists ---
DROP VIEW v2;

DROP VIEW IF EXISTS v2;

DROP TABLE t1;

SHOW TABLES;

SHOW VIEWS;
