-- EXPLAIN-level regression: a datafusion.optimizer.filter_null_join_keys=false
-- override must reach the actual plan, not only SET/SHOW round-trips. The
-- historical default keeps the pre-MergeScan null-key filters, so the default
-- plan keeps the IS NOT NULL filters while the explicit override drops them.
-- All statements run on the MySQL protocol so SET/SHOW/EXPLAIN share one session.
-- Each EXPLAIN redacts volatile peer ids and repartition counts while keeping
-- logical/physical operator semantics visible.
CREATE TABLE fnjk_l (ts TIMESTAMP TIME INDEX, k INT DEFAULT NULL, v INT);
CREATE TABLE fnjk_r (ts TIMESTAMP TIME INDEX, k INT DEFAULT NULL, v INT);

INSERT INTO fnjk_l VALUES ('2099-01-01 00:00:00', 1, 10), ('2099-01-01 00:00:00', NULL, 20);
INSERT INTO fnjk_r VALUES ('2099-01-01 00:00:00', 1, 100), ('2099-01-01 00:00:00', NULL, 200);

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES datafusion.optimizer.filter_null_join_keys;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- SQLNESS PROTOCOL MYSQL
SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- SQLNESS PROTOCOL MYSQL
SET datafusion.optimizer.filter_null_join_keys = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES datafusion.optimizer.filter_null_join_keys;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- Inner-join results are unchanged either way (NULL never matches NULL).
-- SQLNESS PROTOCOL MYSQL
SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- SQLNESS PROTOCOL MYSQL
SET datafusion.optimizer.filter_null_join_keys = true;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES datafusion.optimizer.filter_null_join_keys;

-- SQLNESS PROTOCOL MYSQL
-- SQLNESS REPLACE (peers.*) REDACTED
-- SQLNESS REPLACE (partitioning.*) REDACTED
EXPLAIN SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- SQLNESS PROTOCOL MYSQL
SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

DROP TABLE fnjk_l;
DROP TABLE fnjk_r;
