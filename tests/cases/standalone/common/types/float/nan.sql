-- description: Test usage of the NAN value

-- FLOAT type

-- nan as a constant

SELECT 'NAN'::FLOAT;

-- storing nan in a table

CREATE TABLE floats(f FLOAT, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('NAN'::FLOAT, 1), (1::FLOAT, 2);

SELECT * FROM floats;

-- nan comparison
-- Note, NaN is equals to itself in datafusion and greater than other float or double values.
SELECT 'nan'::FLOAT = 'nan'::FLOAT;

SELECT 'nan'::FLOAT <> 'nan'::FLOAT;

SELECT 'nan'::FLOAT <> 3.0::FLOAT;

-- table filters on nan
-- =

SELECT f FROM floats WHERE f=1;

SELECT f FROM floats WHERE f='nan'::FLOAT;

-- <>

SELECT f FROM floats WHERE f<>1;

SELECT f FROM floats WHERE f<>'nan'::FLOAT;

-- >

SELECT f FROM floats WHERE f>0;

SELECT f FROM floats WHERE f>'nan'::FLOAT;

-- >=

SELECT f FROM floats WHERE f>=1;

SELECT f FROM floats WHERE f>='nan'::FLOAT;

-- <

SELECT f FROM floats WHERE f<1;

SELECT f FROM floats WHERE f<'nan'::FLOAT;

-- <=

SELECT f FROM floats WHERE f<=1;

SELECT f FROM floats WHERE f<='nan'::FLOAT;

DROP TABLE floats;


-- DOUBLE type

-- nan as a constant

SELECT 'NAN'::DOUBLE;

-- storing nan in a table

CREATE TABLE doubles(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('NAN'::DOUBLE, 1), (1::DOUBLE, 2);

SELECT * FROM doubles;

-- nan comparison

SELECT 'nan'::DOUBLE = 'nan'::DOUBLE;

SELECT 'nan'::DOUBLE <> 'nan'::DOUBLE;

SELECT 'nan'::DOUBLE <> 3.0::DOUBLE;

-- table filters on nan
-- =

SELECT d FROM doubles WHERE d=1;

SELECT d FROM doubles WHERE d='nan'::DOUBLE;

-- <>

SELECT d FROM doubles WHERE d<>1;

SELECT d FROM doubles WHERE d<>'nan'::DOUBLE;

-- >

SELECT d FROM doubles WHERE d>0;

SELECT d FROM doubles WHERE d>'nan'::DOUBLE;

-- >=

SELECT d FROM doubles WHERE d>=1;

SELECT d FROM doubles WHERE d>='nan'::DOUBLE;

-- <

SELECT d FROM doubles WHERE d<1;

SELECT d FROM doubles WHERE d<'nan'::DOUBLE;

-- <=

SELECT d FROM doubles WHERE d<=1;

SELECT d FROM doubles WHERE d<='nan'::DOUBLE;

DROP TABLE doubles;
