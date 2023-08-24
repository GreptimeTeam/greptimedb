-- description: Test aggregates on NaN values

-- float --
SELECT SUM('inf'::FLOAT), SUM('-inf'::FLOAT), SUM('nan'::FLOAT);

SELECT MIN('inf'::FLOAT), MIN('-inf'::FLOAT), MIN('nan'::FLOAT);

SELECT MAX('inf'::FLOAT), MAX('-inf'::FLOAT), MAX('nan'::FLOAT);

CREATE TABLE floats(f FLOAT, ts TIMESTAMP TIME INDEX);
INSERT INTO floats VALUES ('inf'::FLOAT, 1), ('-inf'::FLOAT, 2), ('nan'::FLOAT, 3);

SELECT MIN(f), MAX(f) FROM floats;

DROP TABLE floats;

-- double --
SELECT SUM('inf'::DOUBLE), SUM('-inf'::DOUBLE), SUM('nan'::DOUBLE);

SELECT MIN('inf'::DOUBLE), MIN('-inf'::DOUBLE), MIN('nan'::DOUBLE);

SELECT MAX('inf'::DOUBLE), MAX('-inf'::DOUBLE), MAX('nan'::DOUBLE);

CREATE TABLE doubles(f DOUBLE, ts TIMESTAMP TIME INDEX);
INSERT INTO doubles VALUES ('inf'::DOUBLE, 1), ('-inf'::DOUBLE, 2), ('nan'::DOUBLE, 3);

SELECT MIN(f), MAX(f) FROM doubles;

DROP TABLE doubles;
