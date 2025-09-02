-- Tests self-join patterns

CREATE TABLE employee_hierarchy(emp_id INTEGER, emp_name VARCHAR, manager_id INTEGER, level_num INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO employee_hierarchy VALUES 
(1, 'CEO', NULL, 1, 1000), (2, 'CTO', 1, 2, 2000), (3, 'Dev1', 2, 3, 3000), (4, 'Dev2', 2, 3, 4000), (5, 'QA1', 2, 3, 5000);

SELECT e.emp_name as employee, m.emp_name as manager FROM employee_hierarchy e LEFT JOIN employee_hierarchy m ON e.manager_id = m.emp_id ORDER BY e.level_num, e.emp_id;
SELECT m.emp_name as manager, COUNT(e.emp_id) as direct_reports FROM employee_hierarchy m LEFT JOIN employee_hierarchy e ON m.emp_id = e.manager_id GROUP BY m.emp_id, m.emp_name ORDER BY direct_reports DESC, manager DESC;

DROP TABLE employee_hierarchy;