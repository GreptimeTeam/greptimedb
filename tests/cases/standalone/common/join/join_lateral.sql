-- Tests lateral join patterns and correlated subqueries

CREATE TABLE departments_lat(dept_id INTEGER, dept_name VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE employees_lat(emp_id INTEGER, dept_id INTEGER, salary INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO departments_lat VALUES (1, 'Engineering', 1000), (2, 'Sales', 2000), (3, 'Marketing', 3000);

INSERT INTO employees_lat VALUES (1, 1, 75000, 1000), (2, 1, 80000, 2000), (3, 2, 65000, 3000), (4, 2, 70000, 4000), (5, 3, 60000, 5000);

-- Correlated subquery simulating lateral join behavior
SELECT d.dept_name, top_earners.emp_id, top_earners.salary 
FROM departments_lat d
INNER JOIN (
  SELECT emp_id, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
  FROM employees_lat  
) top_earners ON d.dept_id = top_earners.dept_id AND top_earners.rn <= 2
ORDER BY d.dept_id, top_earners.salary DESC;

DROP TABLE departments_lat;

DROP TABLE employees_lat;