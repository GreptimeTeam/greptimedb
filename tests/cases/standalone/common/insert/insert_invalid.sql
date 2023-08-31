CREATE TABLE strings(i STRING, t TIMESTAMP, time index(t));

INSERT INTO strings VALUES ('â‚(', 1);

INSERT INTO strings VALUES (3, 4);

SELECT * FROM strings WHERE i = 'â‚(';

CREATE TABLE a(i integer, j TIMESTAMP, time index(j));

INSERT INTO a VALUES (1, 2);

INSERT INTO a VALUES (1);

INSERT INTO a VALUES (1,2,3);

INSERT INTO a VALUES (1,2),(3);

INSERT INTO a VALUES (1,2),(3,4,5);

DROP TABLE strings;

DROP TABLE a;
