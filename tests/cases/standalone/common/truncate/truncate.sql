
CREATE TABLE monitor (host STRING, ts TIMESTAMP, cpu DOUBLE DEFAULT 0, memory DOUBLE, TIME INDEX (ts), PRIMARY KEY(host));

INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1691746332000, 'host1', 66.6, 1024),
(1691746332000, 'host2', 66.6, 1024),
(1691746332000, 'host3', 66.6, 1024),
(1691746333000, 'host1', 77.7, 2048),
(1691746333000, 'host2', 77.7, 2048),
(1691746333000, 'host3', 77.7, 2048),
(1691746334000, 'host1', 88.8, 4096),
(1691746334000, 'host2', 88.8, 4096),
(1691746334000, 'host3', 88.8, 4096);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

TRUNCATE monitor;

INSERT INTO monitor(ts, host, cpu, memory) VALUES 
(1691746335000, 'host1', 99.9, 8192),
(1691746335000, 'host2', 99.9, 8192),
(1691746335000, 'host3', 99.9, 8192);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

DROP TABLE monitor;
