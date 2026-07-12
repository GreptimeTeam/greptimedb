-- description: Test usage of the INF value
-- FLOAT type

-- inf as a constant
SELECT 'INF'::FLOAT, '-INF'::FLOAT;

-- inf comparison
SELECT 'inf'::FLOAT = 'inf'::FLOAT;

SELECT 'inf'::FLOAT <> 'inf'::FLOAT;

SELECT 'inf'::FLOAT <> 3.0::FLOAT;

-- storing inf in a table
CREATE TABLE floats(f FLOAT PRIMARY KEY, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('INF'::FLOAT, 1), (1::FLOAT, 2), ('-INF'::FLOAT, 3);

SELECT * FROM floats;

-- table filters on inf
-- =
SELECT f FROM floats WHERE f=1;

SELECT f FROM floats WHERE f='inf'::FLOAT;

SELECT f FROM floats WHERE f='-inf'::FLOAT;

-- <>
SELECT f FROM floats WHERE f<>1 ORDER BY 1;

SELECT f FROM floats WHERE f<>'inf'::FLOAT ORDER BY 1;

SELECT f FROM floats WHERE f<>'-inf'::FLOAT ORDER BY 1;

-- >
SELECT f FROM floats WHERE f>1 ORDER BY 1;

SELECT f FROM floats WHERE f>'-inf'::FLOAT ORDER BY 1;

SELECT f FROM floats WHERE f>'inf'::FLOAT;

-- >=
SELECT f FROM floats WHERE f>=1 ORDER BY f;

SELECT f FROM floats WHERE f>='-inf'::FLOAT ORDER BY f;

SELECT f FROM floats WHERE f>='inf'::FLOAT ORDER BY f;

-- <
SELECT f FROM floats WHERE f<1;

SELECT f FROM floats WHERE f<'inf'::FLOAT ORDER BY f;

SELECT f FROM floats WHERE f<'-inf'::FLOAT;

-- <=
SELECT f FROM floats WHERE f<=1 ORDER BY f;

SELECT f FROM floats WHERE f<='inf'::FLOAT ORDER BY f;

SELECT f FROM floats WHERE f<='-inf'::FLOAT ORDER BY f;

DROP TABLE floats;


-- DOUBLE type

-- inf as a constant
SELECT 'INF'::DOUBLE, '-INF'::DOUBLE;

-- inf comparison
SELECT 'inf'::DOUBLE = 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE <> 'inf'::DOUBLE;

SELECT 'inf'::DOUBLE <> 3.0::DOUBLE;

-- storing inf in a table
CREATE TABLE doubles(d DOUBLE PRIMARY KEY, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('INF'::DOUBLE, 1), (1::DOUBLE, 2), ('-INF'::DOUBLE, 3);

SELECT * FROM doubles;

-- table filters on inf
-- =
SELECT d FROM doubles WHERE d=1;

SELECT d FROM doubles WHERE d='inf'::DOUBLE;

SELECT d FROM doubles WHERE d='-inf'::DOUBLE;

-- <>
SELECT d FROM doubles WHERE d<>1 ORDER BY 1;

SELECT d FROM doubles WHERE d<>'inf'::DOUBLE ORDER BY 1;

SELECT d FROM doubles WHERE d<>'-inf'::DOUBLE ORDER BY 1;

-- >
SELECT d FROM doubles WHERE d>1 ORDER BY 1;

SELECT d FROM doubles WHERE d>'-inf'::DOUBLE ORDER BY 1;

SELECT d FROM doubles WHERE d>'inf'::DOUBLE;

-- >=
SELECT d FROM doubles WHERE d>=1 ORDER BY d;

SELECT d FROM doubles WHERE d>='-inf'::DOUBLE ORDER BY d;

SELECT d FROM doubles WHERE d>='inf'::DOUBLE ORDER BY d;

-- <
SELECT d FROM doubles WHERE d<1;

SELECT d FROM doubles WHERE d<'inf'::DOUBLE ORDER BY d;

SELECT d FROM doubles WHERE d<'-inf'::DOUBLE;

-- <=
SELECT d FROM doubles WHERE d<=1 ORDER BY d;

SELECT d FROM doubles WHERE d<='inf'::DOUBLE ORDER BY d;

SELECT d FROM doubles WHERE d<='-inf'::DOUBLE ORDER BY d;

DROP TABLE doubles;
