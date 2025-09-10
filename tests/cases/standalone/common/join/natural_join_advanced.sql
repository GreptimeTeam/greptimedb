-- Migrated from DuckDB test: test/sql/join/natural/ advanced tests
-- Tests advanced natural join patterns

CREATE TABLE employees_nat(emp_id INTEGER, "name" VARCHAR, dept_id INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE departments_nat(dept_id INTEGER, dept_name VARCHAR, budget INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE dept_locations(dept_id INTEGER, "location" VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO employees_nat VALUES
(1, 'Alice', 10, 1000), (2, 'Bob', 20, 2000), (3, 'Charlie', 10, 3000),
(4, 'Diana', 30, 4000), (5, 'Eve', 20, 5000);

INSERT INTO departments_nat VALUES
(10, 'Engineering', 500000, 1000), (20, 'Marketing', 300000, 2000), (30, 'Sales', 200000, 3000);

INSERT INTO dept_locations VALUES
(10, 'Building A', 1000), (20, 'Building B', 2000), (30, 'Building C', 3000);

-- Basic natural join
SELECT * FROM employees_nat NATURAL JOIN departments_nat ORDER BY emp_id;

-- Natural join with filtering
SELECT * FROM employees_nat NATURAL JOIN departments_nat
WHERE budget > 250000 ORDER BY emp_id;

-- Multi-table natural join
SELECT
  emp_id, "name", dept_name, "location", budget
FROM employees_nat
NATURAL JOIN departments_nat
NATURAL JOIN dept_locations
ORDER BY emp_id;

-- Natural join with aggregation
SELECT
  dept_name,
  COUNT(emp_id) as employee_count,
  AVG(budget) as avg_budget,
  "location"
FROM employees_nat
NATURAL JOIN departments_nat
NATURAL JOIN dept_locations
GROUP BY dept_name, "location", budget
ORDER BY employee_count DESC, dept_name ASC;

-- Natural join with expressions
SELECT
  "name",
  dept_name,
  budget,
  CASE WHEN budget > 400000 THEN 'High Budget' ELSE 'Normal Budget' END as budget_tier
FROM employees_nat NATURAL JOIN departments_nat
ORDER BY budget DESC, "name";

DROP TABLE employees_nat;

DROP TABLE departments_nat;

DROP TABLE dept_locations;
