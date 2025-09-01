-- Migrated from DuckDB test: test/sql/window/test_basic_window.test
-- Description: Most basic window function

CREATE TABLE empsalary (depname varchar, empno bigint, salary int, enroll_date date, ts TIMESTAMP TIME INDEX);

INSERT INTO empsalary VALUES 
('develop', 10, 5200, '2007-08-01', 1000),
('sales', 1, 5000, '2006-10-01', 2000),
('personnel', 5, 3500, '2007-12-10', 3000),
('sales', 4, 4800, '2007-08-08', 4000),
('personnel', 2, 3900, '2006-12-23', 5000),
('develop', 7, 4200, '2008-01-01', 6000),
('develop', 9, 4500, '2008-01-01', 7000),
('sales', 3, 4800, '2007-08-01', 8000),
('develop', 8, 6000, '2006-10-01', 9000),
('develop', 11, 5200, '2007-08-15', 10000);

-- Basic window function: SUM with PARTITION BY and ORDER BY
SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname ORDER BY empno) 
FROM empsalary 
ORDER BY depname, empno;

-- SUM with different ordering
SELECT sum(salary) OVER (PARTITION BY depname ORDER BY salary) as ss 
FROM empsalary 
ORDER BY depname, ss;

-- ROW_NUMBER function
SELECT row_number() OVER (PARTITION BY depname ORDER BY salary) as rn 
FROM empsalary 
ORDER BY depname, rn;

-- FIRST_VALUE function
SELECT empno, first_value(empno) OVER (PARTITION BY depname ORDER BY empno) as fv 
FROM empsalary 
ORDER BY fv DESC, empno ASC;

-- Clean up
DROP TABLE empsalary;