-- Migrated from DuckDB test: test/sql/join/left_outer/test_left_outer.test
-- Tests LEFT OUTER JOIN functionality

CREATE TABLE left_t (a INTEGER, b INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE right_t (a INTEGER, c INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO left_t VALUES (1, 10, 1000), (2, 20, 2000), (3, 30, 3000);

INSERT INTO right_t VALUES (1, 100, 4000), (2, 200, 5000), (4, 400, 6000);

-- Basic LEFT JOIN
SELECT * FROM left_t LEFT JOIN right_t ON left_t.a = right_t.a ORDER BY left_t.a;

-- LEFT JOIN with WHERE on left table
SELECT * FROM left_t LEFT JOIN right_t ON left_t.a = right_t.a WHERE left_t.b > 15 ORDER BY left_t.a;

-- LEFT JOIN with WHERE on joined result
SELECT * FROM left_t LEFT JOIN right_t ON left_t.a = right_t.a WHERE right_t.c IS NULL ORDER BY left_t.a;

-- LEFT JOIN with complex condition
SELECT * FROM left_t LEFT JOIN right_t ON left_t.a = right_t.a AND left_t.b < 25 ORDER BY left_t.a;

DROP TABLE right_t;

DROP TABLE left_t;