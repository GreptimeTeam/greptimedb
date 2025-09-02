-- Migrated from DuckDB test: test/sql/join/cross_product/test_cross_product.test
-- Tests CROSS JOIN functionality

CREATE TABLE small_table (a INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE another_table (b INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO small_table VALUES (1, 1000), (2, 2000);

INSERT INTO another_table VALUES (10, 3000), (20, 4000), (30, 5000);

-- Basic CROSS JOIN
SELECT * FROM small_table CROSS JOIN another_table ORDER BY a, b;

-- CROSS JOIN with WHERE filter
SELECT * FROM small_table CROSS JOIN another_table WHERE a + b < 25 ORDER BY a, b;

-- CROSS JOIN with aliases
SELECT s.a, t.b FROM small_table s CROSS JOIN another_table t WHERE s.a = 1 ORDER BY b;

DROP TABLE another_table;

DROP TABLE small_table;