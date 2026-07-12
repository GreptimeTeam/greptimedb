-- Migrated from DuckDB test: test/sql/window/test_window_rows.test
-- Tests window frame specifications

CREATE TABLE t3(a VARCHAR, b VARCHAR, c INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO t3 VALUES
('AA', 'aa', 100, 1000), ('BB', 'aa', 200, 2000), ('CC', 'bb', 300, 3000),
('DD', 'aa', 400, 4000), ('EE', 'bb', 500, 5000);

-- Window with ROWS frame
SELECT a, c, SUM(c) OVER (ORDER BY c ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_val
FROM t3 ORDER BY c;

-- Window with UNBOUNDED frame
SELECT a, c, SUM(c) OVER (ORDER BY c ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumsum
FROM t3 ORDER BY c;

-- Window with partition and frame
SELECT a, b, c, AVG(c) OVER (PARTITION BY b ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as avg_val
FROM t3 ORDER BY b, c;

DROP TABLE t3;
