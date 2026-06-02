SELECT timestamp '    2017-07-23    13:10:11   ';

SELECT timestamp '    2017-07-23 13:10:11    ';

CREATE TABLE timestamp_with_precision (ts TIMESTAMP(6) TIME INDEX, cnt INT);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0000', 1);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2023-04-04 08:00:00.0052+0800', 2);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+100000-01-01 00:00:01.5Z', 3);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262143-01-01 00:00:00Z', 4);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262142-12-31 23:59:59Z', 5);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262143-01-01 00:00:00Z', 6);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262142-12-31 23:59:59.999Z', 7);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('-262143-01-01 00:00:00Z', 8);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('+262142-12-31 23:59:59.999999Z', 9);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('1677-09-21 00:12:43.145225Z', 10);

INSERT INTO timestamp_with_precision(ts,cnt) VALUES ('2262-04-11 23:47:16.854775807Z', 11);

SELECT * FROM timestamp_with_precision ORDER BY ts ASC;

DROP TABLE timestamp_with_precision;

CREATE TABLE plain_timestamp (ts TIMESTAMP TIME INDEX);

INSERT INTO plain_timestamp VALUES (1);

SELECT * FROM plain_timestamp;

SELECT * FROM plain_timestamp where ts = '1970-01-01 00:00:00.001000';

DROP TABLE plain_timestamp;

-- Regression test for https://github.com/GreptimeTeam/greptimedb/issues/8214:
-- string timestamp predicates must align to the time index precision before comparison.
CREATE TABLE ts_precision_bug (ts TIMESTAMP(3) TIME INDEX, v INT, PRIMARY KEY (v));

INSERT INTO ts_precision_bug VALUES
('2026-06-02 03:49:59.999', 1),
('2026-06-02 03:50:00.000', 2),
('2026-06-02 03:50:00.195', 3),
('2026-06-02 03:50:01.000', 4);

SELECT ts, v
FROM ts_precision_bug
WHERE ts <= '2026-06-02 03:50:00'
ORDER BY ts DESC
LIMIT 1;

SELECT date_format(ts, '%Y-%m-%d %H:%M:%S:%3f') AS ts, v
FROM ts_precision_bug
WHERE ts <= '2026-06-02 03:50:00'
ORDER BY ts DESC
LIMIT 1;

DROP TABLE ts_precision_bug;
