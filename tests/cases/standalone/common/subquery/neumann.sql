-- Migrated from DuckDB test: test/sql/subquery/test_neumann.test
-- Description: Test subqueries from the paper 'Unnesting Arbitrary Subqueries'

CREATE TABLE students("id" INTEGER, name VARCHAR, major VARCHAR, "year" INTEGER, ts TIMESTAMP TIME INDEX);

CREATE TABLE exams(sid INTEGER, course VARCHAR, curriculum VARCHAR, grade INTEGER, "year" INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO students VALUES (1, 'Mark', 'CS', 2017, 1000);

INSERT INTO students VALUES (2, 'Dirk', 'CS', 2017, 2000);

INSERT INTO exams VALUES (1, 'Database Systems', 'CS', 10, 2015, 3000);

INSERT INTO exams VALUES (1, 'Graphics', 'CS', 9, 2016, 4000);

INSERT INTO exams VALUES (2, 'Database Systems', 'CS', 7, 2015, 5000);

INSERT INTO exams VALUES (2, 'Graphics', 'CS', 7, 2016, 6000);

-- Test 1: Subquery with MAX aggregation
SELECT s.name, e.course, e.grade 
FROM students s, exams e 
WHERE s."id"=e.sid AND e.grade=(SELECT MAX(e2.grade) FROM exams e2 WHERE s."id"=e2.sid) 
ORDER BY name, course;

-- Test 2: Complex subquery with AVG and conditions
SELECT s.name, e.course, e.grade 
FROM students s, exams e 
WHERE s."id"=e.sid 
  AND (s.major = 'CS' OR s.major = 'Games Eng') 
  AND e.grade <= (SELECT AVG(e2.grade) - 1 FROM exams e2 WHERE s."id"=e2.sid OR (e2.curriculum=s.major AND s."year">=e2."year")) 
ORDER BY name, course;

-- Test 3: EXISTS subquery
SELECT name, major 
FROM students s 
WHERE EXISTS(SELECT * FROM exams e WHERE e.sid=s."id" AND grade=10) OR s.name='Dirk' 
ORDER BY name;

-- Clean up
DROP TABLE exams;

DROP TABLE students;
