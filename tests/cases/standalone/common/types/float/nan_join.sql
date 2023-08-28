-- description: Test NaN and inf joins
-- group: [float]

-- FLOAT type

CREATE TABLE floats(f FLOAT, ts TIMESTAMP TIME INDEX);

INSERT INTO floats VALUES ('inf'::FLOAT, 1), ('-inf'::FLOAT, 2), ('nan'::FLOAT, 3);

SELECT * FROM floats JOIN (SELECT 'inf'::FLOAT) tbl(f) USING (f);

SELECT * FROM floats JOIN (SELECT '-inf'::FLOAT) tbl(f) USING (f);

SELECT * FROM floats JOIN (SELECT 'inf'::FLOAT) tbl(f) ON (floats.f >= tbl.f) ORDER BY 1;

SELECT * FROM floats JOIN (SELECT 'inf'::FLOAT) tbl(f) ON (floats.f <= tbl.f) ORDER BY 1;

SELECT * FROM floats JOIN (SELECT '-inf'::FLOAT, 'inf'::FLOAT) tbl(f,g) ON (floats.f >= tbl.f AND floats.f <= tbl.g) ORDER BY 1;

DROP TABLE floats;


-- DOUBLE type

CREATE TABLE doubles(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES ('inf'::DOUBLE, 1), ('-inf'::DOUBLE, 2), ('nan'::DOUBLE, 3);

SELECT * FROM doubles JOIN (SELECT 'inf'::DOUBLE) tbl(d) USING (d);

SELECT * FROM doubles JOIN (SELECT '-inf'::DOUBLE) tbl(d) USING (d);

SELECT * FROM doubles JOIN (SELECT 'inf'::DOUBLE) tbl(d) ON (doubles.d >= tbl.d) ORDER BY 1;

SELECT * FROM doubles JOIN (SELECT 'inf'::DOUBLE) tbl(d) ON (doubles.d <= tbl.d) ORDER BY 1;

SELECT * FROM doubles JOIN (SELECT '-inf'::DOUBLE, 'inf'::DOUBLE) tbl(d,g) ON (doubles.d >= tbl.d AND doubles.d <= tbl.g) ORDER BY 1;

DROP TABLE doubles;
