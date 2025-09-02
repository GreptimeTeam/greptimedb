-- Migrated from DuckDB test: Self join scenarios
-- Tests self join operations

CREATE TABLE employees_self(
  "id" INTEGER,
  "name" VARCHAR,
  manager_id INTEGER,
  salary INTEGER,
  ts TIMESTAMP TIME INDEX
);

INSERT INTO employees_self VALUES 
(1, 'CEO', NULL, 100000, 1000),
(2, 'Manager1', 1, 80000, 2000),
(3, 'Manager2', 1, 75000, 3000),
(4, 'Employee1', 2, 50000, 4000),
(5, 'Employee2', 2, 55000, 5000),
(6, 'Employee3', 3, 48000, 6000);

-- Basic self join to get employee-manager pairs
SELECT e."name" as employee, m."name" as manager
FROM employees_self e
LEFT JOIN employees_self m ON e.manager_id = m."id"
ORDER BY e."id";

-- Self join to find employees earning more than their manager
SELECT e."name" as employee, e.salary, m."name" as manager, m.salary as manager_salary
FROM employees_self e
JOIN employees_self m ON e.manager_id = m."id"
WHERE e.salary > m.salary
ORDER BY e."name";

-- Self join to find colleagues (same manager)
SELECT e1."name" as employee1, e2."name" as employee2, m."name" as shared_manager
FROM employees_self e1
JOIN employees_self e2 ON e1.manager_id = e2.manager_id AND e1."id" < e2."id"
JOIN employees_self m ON e1.manager_id = m."id"
ORDER BY shared_manager, employee1;

-- Hierarchical query using self join
SELECT 
  e."name" as employee,
  e.salary,
  m."name" as manager,
  COUNT(sub."id") as direct_reports
FROM employees_self e
LEFT JOIN employees_self m ON e.manager_id = m."id"
LEFT JOIN employees_self sub ON e."id" = sub.manager_id
GROUP BY e."id", e."name", e.salary, m."name"
ORDER BY e."id";

DROP TABLE employees_self;