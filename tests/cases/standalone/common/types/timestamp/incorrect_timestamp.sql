CREATE TABLE timestamp(t TIMESTAMP time index);

INSERT INTO timestamp VALUES ('blabla');

INSERT INTO timestamp VALUES ('1993-20-14 00:00:00');

INSERT INTO timestamp VALUES ('1993-08-99 00:00:00');

INSERT INTO timestamp VALUES ('1993-02-29 00:00:00');

INSERT INTO timestamp VALUES ('1900-02-29 00:00:00');

INSERT INTO timestamp VALUES ('1992-02-29 00:00:00');

INSERT INTO timestamp VALUES ('2000-02-29 00:00:00');

INSERT INTO timestamp VALUES ('02-02-1992 00:00:00');

INSERT INTO timestamp VALUES ('1900-1-1 59:59:23');

INSERT INTO timestamp VALUES ('1900a01a01 00:00:00');

INSERT INTO timestamp VALUES ('1900-1-1 00;00;00');

INSERT INTO timestamp VALUES ('1900-1-1 00a00a00');

INSERT INTO timestamp VALUES ('1900-1-1 00/00/00');

INSERT INTO timestamp VALUES ('1900-1-1 00-00-00');

DROP TABLE timestamp;
