-- Migrated from DuckDB test: Multiple window aggregate tests
-- Tests window aggregate functions

CREATE TABLE sales("region" VARCHAR, "quarter" INTEGER, amount INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO sales VALUES 
('North', 1, 1000, 1000), ('North', 2, 1200, 2000), ('North', 3, 1100, 3000),
('South', 1, 800, 4000), ('South', 2, 900, 5000), ('South', 3, 950, 6000);

-- Running totals with SUM window function
SELECT region, "quarter", amount, 
  SUM(amount) OVER (PARTITION BY region ORDER BY "quarter") as running_total
FROM sales ORDER BY region, "quarter";

-- Moving averages with AVG window function
SELECT region, "quarter", amount,
  AVG(amount) OVER (PARTITION BY region ORDER BY "quarter" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg
FROM sales ORDER BY region, "quarter";

-- MIN/MAX window functions
SELECT region, "quarter", amount,
  MIN(amount) OVER (PARTITION BY region) as min_amount,
  MAX(amount) OVER (PARTITION BY region) as max_amount
FROM sales ORDER BY region, "quarter";

-- COUNT window function
SELECT region, "quarter", 
  COUNT(*) OVER (PARTITION BY region) as region_count,
  COUNT(*) OVER () as total_count
FROM sales ORDER BY region, "quarter";

DROP TABLE sales;
