CREATE TABLE students(ts TIMESTAMP TIME INDEX, id INTEGER, n VARCHAR, major VARCHAR, y INTEGER);

CREATE TABLE exams(ts TIMESTAMP TIME INDEX, sid INTEGER, course VARCHAR, curriculum VARCHAR, grade INTEGER, y INTEGER);

INSERT INTO students VALUES (1, 1, 'Mark', 'CS', 2017);

INSERT INTO students VALUES (2, 2, 'Dirk', 'CS', 2017);

INSERT INTO exams VALUES (1, 1, 'Database Systems', 'CS', 10, 2015);

INSERT INTO exams VALUES (2, 1, 'Graphics', 'CS', 9, 2016);

INSERT INTO exams VALUES (3, 2, 'Database Systems', 'CS', 7, 2015);

INSERT INTO exams VALUES (4, 2, 'Graphics', 'CS', 7, 2016);

SELECT s.n, e.course, e.grade FROM students s, exams e WHERE s.id=e.sid AND e.grade=(SELECT MAX(e2.grade) FROM exams e2 WHERE s.id=e2.sid) ORDER BY n, course;

SELECT n, major FROM students s WHERE EXISTS(SELECT * FROM exams e WHERE e.sid=s.id AND grade=10) OR s.n='Dirk' ORDER BY n;

DROP TABLE students;
