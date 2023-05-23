CREATE TABLE timestamp_with_precision (ts TIMESTAMP(6) TIME INDEX, cnt INT);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0000', 1);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0800', 2);

SELECT * FROM timestamp_with_precision ORDER BY ts ASC;

DROP TABLE timestamp_with_precision;
