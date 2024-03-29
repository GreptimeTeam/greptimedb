CREATE TABLE monitor (host STRING NOT NULL, ts TIMESTAMP NOT NULL, cpu DOUBLE NOT NULL, memory DOUBLE NOT NULL, TIME INDEX (ts), PRIMARY KEY(host));

INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1655276557000, 'host1', 10.0, 1024),
(1655276557000, 'host2', 20.0, 1024),
(1655276557000, 'host3', 30.0, 1024);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts, host;

DELETE FROM monitor WHERE host = 'host2';

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts, host;

DROP TABLE monitor;
