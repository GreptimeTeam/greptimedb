CREATE TABLE insert_invalid_strings(i STRING, t BIGINT, time index(t));

INSERT INTO insert_invalid_strings VALUES ('â‚(', 1);

INSERT INTO insert_invalid_strings VALUES (3, 4);

SELECT * FROM insert_invalid_strings WHERE i = 'â‚(';

CREATE TABLE a(i integer, j BIGINT, time index(j));

INSERT INTO a VALUES (1, 2);

INSERT INTO a VALUES (1);

INSERT INTO a VALUES (1,2,3);

INSERT INTO a VALUES (1,2),(3);

INSERT INTO a VALUES (1,2),(3,4,5);
