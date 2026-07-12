-- Migrated from DuckDB test: test/sql/join/ complex condition tests
-- Tests complex join conditions and predicates

CREATE TABLE sales_reps(rep_id INTEGER, "name" VARCHAR, "region" VARCHAR, quota INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE customer_accounts(account_id INTEGER, account_name VARCHAR, "region" VARCHAR, rep_id INTEGER, revenue INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO sales_reps VALUES
(1, 'Tom', 'North', 100000, 1000), (2, 'Sarah', 'South', 150000, 2000),
(3, 'Mike', 'East', 120000, 3000), (4, 'Lisa', 'West', 180000, 4000);

INSERT INTO customer_accounts VALUES
(101, 'TechCorp', 'North', 1, 85000, 1000), (102, 'DataInc', 'South', 2, 195000, 2000),
(103, 'CloudSys', 'North', 1, 110000, 3000), (104, 'NetSoft', 'East', 3, 75000, 4000),
(105, 'WebCo', 'West', 4, 225000, 5000), (106, 'AppDev', 'South', 2, 140000, 6000);

-- Join with multiple conditions
SELECT
  sr."name" as rep_name, ca.account_name, ca.revenue
FROM sales_reps sr
INNER JOIN customer_accounts ca
  ON sr.rep_id = ca.rep_id AND sr.region = ca.region
ORDER BY sr.rep_id, ca.revenue DESC;

-- Join with inequality conditions
SELECT
  sr."name", sr.quota, ca.account_name, ca.revenue
FROM sales_reps sr
INNER JOIN customer_accounts ca
  ON sr.rep_id = ca.rep_id AND ca.revenue < sr.quota
ORDER BY sr.rep_id, ca.revenue;

-- Join with range conditions
SELECT
  sr."name", ca.account_name, ca.revenue, sr.quota
FROM sales_reps sr
INNER JOIN customer_accounts ca
  ON sr.rep_id = ca.rep_id
  AND ca.revenue BETWEEN sr.quota * 0.5 AND sr.quota * 1.5
ORDER BY sr.rep_id, ca.revenue;

-- Join with CASE in conditions
SELECT
  sr."name", ca.account_name, ca.revenue,
  CASE WHEN ca.revenue >= sr.quota THEN 'Met Quota' ELSE 'Below Quota' END as performance
FROM sales_reps sr
INNER JOIN customer_accounts ca ON sr.rep_id = ca.rep_id
ORDER BY sr.rep_id, ca.revenue DESC;

-- Join with expression conditions
SELECT
  sr."name", ca.account_name,
  ca.revenue, sr.quota,
  ca.revenue - sr.quota as quota_diff
FROM sales_reps sr
INNER JOIN customer_accounts ca
  ON sr.rep_id = ca.rep_id
  AND UPPER(sr.region) = UPPER(ca.region)
ORDER BY quota_diff DESC, sr."name" ASC;

-- Join with string pattern conditions
SELECT
  sr."name", ca.account_name
FROM sales_reps sr
INNER JOIN customer_accounts ca
  ON sr.rep_id = ca.rep_id
  AND ca.account_name LIKE '%Corp%'
ORDER BY sr."name";

-- Complex nested join conditions
SELECT
  sr."name", ca.account_name, ca.revenue
FROM sales_reps sr
INNER JOIN customer_accounts ca ON (
  sr.rep_id = ca.rep_id
  AND (ca.revenue > 100000 OR sr.quota < 130000)
  AND sr.region IN ('North', 'South')
)
ORDER BY ca.revenue DESC;

DROP TABLE sales_reps;

DROP TABLE customer_accounts;
