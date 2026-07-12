-- description: Test ordering of NaN and infinity values

-- FLOAT type

-- storing nan in a table
CREATE TABLE floats(f FLOAT, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('NAN'::FLOAT, 1), (1::FLOAT, 2), ('infinity'::FLOAT, 3), ('-infinity'::FLOAT, 4), (-1::FLOAT, 5), (NULL, 6);

-- standard ordering
SELECT f FROM floats ORDER BY f;

SELECT f FROM floats ORDER BY f DESC;

-- top-n
SELECT f FROM floats ORDER BY f DESC NULLS LAST LIMIT 2;

SELECT f FROM floats ORDER BY f NULLS LAST LIMIT 2;

SELECT f FROM floats ORDER BY f DESC NULLS LAST LIMIT 4;

SELECT f FROM floats ORDER BY f NULLS LAST LIMIT 4;

-- count with filters
SELECT COUNT(*) FROM floats WHERE f > 0;

SELECT COUNT(*) FROM floats WHERE f < 0;

DROP TABLE floats;


-- DOUBLE type

-- storing nan in a table
CREATE TABLE doubles(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('NAN'::DOUBLE, 1), (1::DOUBLE, 2), ('infinity'::DOUBLE, 3), ('-infinity'::DOUBLE, 4), (-1::DOUBLE, 5), (NULL, 6);


-- standard ordering
SELECT d FROM doubles ORDER BY d;

SELECT d FROM doubles ORDER BY d DESC;

-- top-n
SELECT d FROM doubles ORDER BY d DESC NULLS LAST LIMIT 2;

SELECT d FROM doubles ORDER BY d NULLS LAST LIMIT 2;

SELECT d FROM doubles ORDER BY d DESC NULLS LAST LIMIT 4;

SELECT d FROM doubles ORDER BY d NULLS LAST LIMIT 4;

-- count with filters
SELECT COUNT(*) FROM doubles WHERE d > 0;

SELECT COUNT(*) FROM doubles WHERE d < 0;

DROP TABLE doubles;
