-- Migrated from DuckDB test: test/sql/join/natural/natural_join.test
-- Tests NATURAL JOIN functionality

CREATE TABLE emp_natural("id" INTEGER, "name" VARCHAR, dept_id INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE dept_natural(dept_id INTEGER, dept_name VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO emp_natural VALUES (1, 'Alice', 10, 1000), (2, 'Bob', 20, 2000), (3, 'Carol', 10, 3000);

INSERT INTO dept_natural VALUES (10, 'Engineering', 4000), (20, 'Sales', 5000), (30, 'Marketing', 6000);

-- NATURAL JOIN (joins on common column names)
SELECT * FROM emp_natural NATURAL JOIN dept_natural ORDER BY "id";

-- NATURAL LEFT JOIN
SELECT * FROM emp_natural NATURAL LEFT JOIN dept_natural ORDER BY "id";

-- NATURAL RIGHT JOIN
SELECT * FROM emp_natural NATURAL RIGHT JOIN dept_natural ORDER BY dept_id;

DROP TABLE dept_natural;

DROP TABLE emp_natural;