CREATE TABLE monitor (host STRING, ts TIMESTAMP, cpu DOUBLE DEFAULT 0, memory DOUBLE, TIME INDEX (ts), PRIMARY KEY(host));

INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1655276557000, 'host1', 66.6, 1024),
(1655276557000, 'host2', 66.6, 1024),
(1655276557000, 'host3', 66.6, 1024),
(1655276558000, 'host1', 77.7, 2048),
(1655276558000, 'host2', 77.7, 2048),
(1655276558000, 'host3', 77.7, 2048),
(1655276559000, 'host1', 88.8, 4096),
(1655276559000, 'host2', 88.8, 4096),
(1655276559000, 'host3', 88.8, 4096);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

DELETE FROM monitor WHERE host = 'host1' AND ts = 1655276557000000000::timestamp;

DELETE FROM monitor WHERE host = 'host2';

DELETE FROM monitor WHERE cpu = 66.6;

DELETE FROM monitor WHERE memory > 2048;

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

DROP TABLE monitor;
