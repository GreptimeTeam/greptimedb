-- Migrated from DuckDB test: test/sql/window/test_cume_dist_orderby.test  
-- Tests CUME_DIST and PERCENT_RANK window functions

CREATE TABLE test_rank(x INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO test_rank VALUES 
(1, 1000), (1, 2000), (2, 3000), (2, 4000), (3, 5000), (3, 6000), (4, 7000);

-- CUME_DIST function
SELECT x, CUME_DIST() OVER (ORDER BY x) as cume_dist_val FROM test_rank ORDER BY ts;

-- PERCENT_RANK function  
SELECT x, PERCENT_RANK() OVER (ORDER BY x) as percent_rank_val FROM test_rank ORDER BY ts;

-- Combined with partitioning
SELECT x, 
  CUME_DIST() OVER (PARTITION BY x ORDER BY ts) as cume_dist_partition,
  PERCENT_RANK() OVER (PARTITION BY x ORDER BY ts) as percent_rank_partition
FROM test_rank ORDER BY x, ts;

DROP TABLE test_rank;