CREATE TABLE timestamp1(i TIMESTAMP time index);

CREATE TABLE timestamp2(i TIMESTAMP time index);

INSERT INTO timestamp1 VALUES ('1993-08-14 00:00:01');

INSERT INTO timestamp2 VALUES ('1993-08-14 00:00:01');

select count(*) from timestamp2 inner join timestamp1 on (timestamp1.i = timestamp2.i);

DROP TABLE timestamp1;

DROP TABLE timestamp2;
