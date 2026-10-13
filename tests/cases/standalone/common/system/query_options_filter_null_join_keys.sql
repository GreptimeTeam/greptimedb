-- EXPLAIN-level regression: a datafusion.optimizer.filter_null_join_keys=false
-- override must reach the actual plan, not only SET/SHOW round-trips. The
-- historical default keeps the pre-MergeScan null-key filters, so the default
-- plan keeps the IS NOT NULL filters while the explicit override drops them.
-- All statements run on the MySQL protocol so SET/SHOW/EXPLAIN share one session.
CREATE TABLE fnjk_l (ts TIMESTAMP TIME INDEX, k INT DEFAULT NULL, v INT);
CREATE TABLE fnjk_r (ts TIMESTAMP TIME INDEX, k INT DEFAULT NULL, v INT);

INSERT INTO fnjk_l VALUES (0, 1, 10), (0, NULL, 20);
INSERT INTO fnjk_r VALUES (0, 1, 100), (0, NULL, 200);

-- SQLNESS PROTOCOL MYSQL
SET datafusion.optimizer.filter_null_join_keys = false;

-- SQLNESS PROTOCOL MYSQL
SHOW VARIABLES datafusion.optimizer.filter_null_join_keys;

-- SQLNESS PROTOCOL MYSQL
EXPLAIN SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- Inner-join results are unchanged either way (NULL never matches NULL).
-- SQLNESS PROTOCOL MYSQL
SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;

-- SQLNESS PROTOCOL MYSQL
SET datafusion.optimizer.filter_null_join_keys = true;

-- SQLNESS PROTOCOL MYSQL
EXPLAIN SELECT l.k, l.v, r.v FROM fnjk_l l JOIN fnjk_r r ON l.k = r.k;
