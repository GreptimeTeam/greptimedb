SELECT SUM(number) FROM numbers;

SELECT SUM(1) FROM numbers;

SELECT SUM(-1) FROM numbers;

SELECT SUM(-1) FROM numbers WHERE number=-1;

SELECT SUM(-1) FROM numbers WHERE number>10000 limit 1000;

CREATE TABLE bigints(b bigint, ts TIMESTAMP TIME INDEX);

INSERT INTO bigints values (4611686018427387904, 1), (4611686018427388904, 2), (1, 3);

SELECT SUM(b) FROM bigints;

CREATE TABLE doubles(n DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles (n, ts) VALUES (9007199254740992, 1), (1, 2), (1, 3), (0, 4);

SELECT sum(n) from doubles;

DROP TABLE bigints;

DROP TABLE doubles;
