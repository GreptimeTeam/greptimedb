SELECT timestamp '    2017-07-23    13:10:11   ';

SELECT timestamp '    2017-07-23 13:10:11    ';

CREATE TABLE timestamp_with_precision (ts TIMESTAMP(6) TIME INDEX, cnt INT);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0000', 1);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0800', 2);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+100000-01-01 00:00:01.5Z', 3);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262144-01-01 00:00:00Z', 4);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262143-12-31 23:59:59Z', 5);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262144-01-01 00:00:00Z', 6);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262143-12-31 23:59:59.999Z', 7);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262144-01-01 00:00:00Z', 8);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262143-12-31 23:59:59.999999Z', 9);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('1677-09-21 00:12:43.145225Z', 10);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2262-04-11 23:47:16.854775807Z', 11);

SELECT * FROM timestamp_with_precision ORDER BY ts ASC;

DROP TABLE timestamp_with_precision;

CREATE TABLE plain_timestamp (ts TIMESTAMP TIME INDEX);

INSERT INTO plain_timestamp VALUES (1);

SELECT * FROM plain_timestamp;

SELECT * FROM plain_timestamp where ts = '1970-01-01 00:00:00.001000';

DROP TABLE plain_timestamp;
