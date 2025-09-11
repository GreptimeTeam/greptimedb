-- Migrated from DuckDB test: test/sql/join/full_outer/ complex tests
-- Tests complex outer join scenarios

CREATE TABLE employees(emp_id INTEGER, "name" VARCHAR, dept_id INTEGER, salary INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE departments(dept_id INTEGER, dept_name VARCHAR, manager_id INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE projects(proj_id INTEGER, proj_name VARCHAR, dept_id INTEGER, budget INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO employees VALUES
(1, 'Alice', 10, 75000, 1000), (2, 'Bob', 20, 65000, 2000),
(3, 'Charlie', 10, 80000, 3000), (4, 'Diana', 30, 70000, 4000), (5, 'Eve', NULL, 60000, 5000);

INSERT INTO departments VALUES
(10, 'Engineering', 1, 1000), (20, 'Marketing', 2, 2000), (40, 'HR', NULL, 3000);

INSERT INTO projects VALUES
(101, 'ProjectA', 10, 100000, 1000), (102, 'ProjectB', 20, 150000, 2000),
(103, 'ProjectC', 30, 75000, 3000), (104, 'ProjectD', 50, 200000, 4000);

-- Full outer join with multiple conditions
SELECT
  e.emp_id, e."name", d.dept_id, d.dept_name
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.dept_id
ORDER BY e.emp_id, d.dept_id;

-- Left outer join with IS NULL filter
SELECT
  e.emp_id, e."name", e.dept_id, d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.dept_id
WHERE d.dept_id IS NULL
ORDER BY e.emp_id;

-- Right outer join
SELECT
  e.emp_id, e."name", d.dept_id, d.dept_name
FROM employees e
RIGHT OUTER JOIN departments d ON e.dept_id = d.dept_id
ORDER BY d.dept_id, e.emp_id;

-- Triple outer join
SELECT
  e."name", d.dept_name, p.proj_name, p.budget
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.dept_id
FULL OUTER JOIN projects p ON d.dept_id = p.dept_id
ORDER BY e.emp_id, p.proj_id;

-- Outer join with aggregation
SELECT
  d.dept_name,
  COUNT(e.emp_id) as employee_count,
  AVG(e.salary) as avg_salary,
  SUM(p.budget) as total_project_budget
FROM departments d
LEFT OUTER JOIN employees e ON d.dept_id = e.dept_id
LEFT OUTER JOIN projects p ON d.dept_id = p.dept_id
GROUP BY d.dept_name
ORDER BY d.dept_name;

DROP TABLE employees;

DROP TABLE departments;

DROP TABLE projects;
