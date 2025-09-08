-- Migrated from DuckDB test: test/sql/limit/test_preserve_insertion_order.test  

CREATE TABLE integers(i INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES 
(1, 1000), (1, 2000), (1, 3000), (1, 4000), (1, 5000),
(2, 6000), (3, 7000), (4, 8000), (5, 9000), (10, 10000);

SELECT MIN(i), MAX(i), COUNT(*) FROM integers;

SELECT * FROM integers ORDER BY ts LIMIT 5;

SELECT * FROM integers ORDER BY ts LIMIT 3 OFFSET 2;

SELECT * FROM integers WHERE i IN (1, 3, 5, 10) ORDER BY i;

SELECT * FROM integers WHERE i IN (1, 3, 5, 10) ORDER BY i, ts LIMIT 3;

-- Clean up
DROP TABLE integers;
