CREATE DATABASE schema_for_view_test;

USE schema_for_view_test;

CREATE TABLE t1(a INT, b STRING, c TIMESTAMP TIME INDEX);

INSERT INTO t1 VALUES (41, "hello", 1), (42, "world", 2), (43, "greptime", 3);

CREATE VIEW v1 AS SELECT a, b FROM t1;

SELECT * FROM v1;

SELECT a FROM v1;

INSERT INTO t1 VALUES (44, "greptimedb", 4);

SELECT * FROM v1;

SHOW CREATE VIEW v1;

CREATE OR REPLACE VIEW v1 AS SELECT a, b, c FROM t1 WHERE a > 43;

SHOW CREATE VIEW v1;

SELECT * FROM v1;

--- if not exists, so it doesn't change at all ---
CREATE VIEW IF NOT EXISTS v1 AS SELECT c FROM t1;

SHOW CREATE VIEW v1;

SELECT * FROM v1;

--- if not exists with replace, so it changes ---
CREATE OR REPLACE VIEW IF NOT EXISTS v1 AS SELECT c FROM t1;

SHOW CREATE VIEW v1;

SELECT * FROM v1;


USE public;

DROP DATABASE schema_for_view_test;
