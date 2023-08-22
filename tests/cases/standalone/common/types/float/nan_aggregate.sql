-- description: Test NaN and inf as aggregate groups

-- float
CREATE TABLE floats(f FLOAT, i INT, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('inf'::FLOAT, 1, 2), ('inf'::FLOAT, 7, 3), ('-inf'::FLOAT, 3, 4), ('nan'::FLOAT, 7, 5), ('nan'::FLOAT, 19, 6), ('-inf'::FLOAT, 2, 7);

SELECT f, SUM(i) FROM floats GROUP BY f ORDER BY f;

SELECT SUM(f) FROM floats WHERE f > 0 AND f != 'nan'::FLOAT;

SELECT SUM(f) FROM floats WHERE f < 0;

SELECT SUM(f) FROM floats;

DROP TABLE floats;

-- double
CREATE TABLE doubles(f DOUBLE, i INT, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('inf'::DOUBLE, 1, 2), ('inf'::DOUBLE, 7, 3), ('-inf'::DOUBLE, 3, 4), ('nan'::DOUBLE, 7, 5), ('nan'::DOUBLE, 19, 6), ('-inf'::DOUBLE, 2, 7);

SELECT f, SUM(i) FROM doubles GROUP BY f ORDER BY f;

SELECT SUM(f) FROM doubles WHERE f > 0 AND f != 'nan'::DOUBLE;

SELECT SUM(f) FROM doubles WHERE f < 0;

SELECT SUM(f) FROM doubles;

DROP TABLE doubles;

-- float double
CREATE TABLE floats_doubles (f FLOAT, d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO floats_doubles VALUES (2e38, 1e308, 1), (2e38, 1e308, 2), (-1e38, 0, 3), (-1e38, 0, 4);

SELECT * FROM floats_doubles;

-- not out of range --
SELECT SUM(f) FROM floats_doubles WHERE f > 0;

-- not out of range, but INF --
SELECT SUM(d) FROM floats_doubles WHERE d > 0;

DROP TABLE floats_doubles;
