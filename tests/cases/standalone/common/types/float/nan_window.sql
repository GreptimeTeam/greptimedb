-- description: Test NaN and inf in windowing functions

-- grouping by inf and nan
CREATE TABLE floats(f FLOAT, i INT, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('inf'::FLOAT, 1, 2), ('inf'::FLOAT, 7, 3), ('-inf'::FLOAT, 3, 4), ('nan'::FLOAT, 7, 5), ('nan'::FLOAT, 19, 6), ('-inf'::FLOAT, 2, 7);

SELECT f, SUM(i) OVER (PARTITION BY f) FROM floats ORDER BY f;

SELECT f, i, SUM(i) OVER (ORDER BY f, i) FROM floats ORDER BY f, i;

SELECT f, i, SUM(i) OVER (PARTITION BY f ORDER BY f, i) FROM floats ORDER BY f, i;

SELECT i, f, SUM(i) OVER (ORDER BY i, f) FROM floats ORDER BY i, f;

DROP TABLE floats;

-- grouping by inf and nan
CREATE TABLE doubles(f DOUBLE, i INT, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('inf'::DOUBLE, 1, 2), ('inf'::DOUBLE, 7, 3), ('-inf'::DOUBLE, 3, 4), ('nan'::DOUBLE, 7, 5), ('nan'::DOUBLE, 19, 6), ('-inf'::DOUBLE, 2, 7);


SELECT f, SUM(i) OVER (PARTITION BY f) FROM doubles ORDER BY f;

SELECT f, i, SUM(i) OVER (ORDER BY f, i) FROM doubles ORDER BY f, i;

SELECT f, i, SUM(i) OVER (PARTITION BY f ORDER BY f, i) FROM doubles ORDER BY f, i;

SELECT i, f, SUM(i) OVER (ORDER BY i, f) FROM doubles ORDER BY i, f;

DROP TABLE doubles;
