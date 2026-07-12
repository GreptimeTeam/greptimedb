-- Migrated from DuckDB test: test/sql/window/test_rank.test

CREATE TABLE test_data(i INTEGER, ts TIMESTAMP TIME INDEX);
INSERT INTO test_data VALUES (1, 1000), (1, 2000), (2, 3000), (2, 4000), (3, 5000);

-- RANK function with ties
SELECT i, RANK() OVER (ORDER BY i) as rank_val FROM test_data ORDER BY ts;

-- DENSE_RANK function  
SELECT i, DENSE_RANK() OVER (ORDER BY i) as dense_rank_val FROM test_data ORDER BY ts;

-- ROW_NUMBER function
SELECT i, ROW_NUMBER() OVER (ORDER BY i) as row_num FROM test_data ORDER BY ts;

DROP TABLE test_data;
