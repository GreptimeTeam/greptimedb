CREATE TABLE monitor (host STRING, ts TIMESTAMP, cpu DOUBLE DEFAULT 0, memory DOUBLE, TIME INDEX (ts), PRIMARY KEY(host));

CREATE TABLE avg_1m_monitor (time_window TIMESTAMP, host STRING, cpu DOUBLE, memory DOUBLE, TIME INDEX (time_window), PRIMARY KEY(host));

CREATE FLOW avg_1m_flow SINK TO avg_1m_monitor AS
SELECT date_bin('1 minute', ts) AS time_window, host, avg(cpu) AS cpu, avg(memory) AS memory FROM monitor
GROUP BY time_window, host;

INSERT INTO monitor(ts, host, cpu, memory) VALUES
("2021-07-01 00:00:00.200", 'host1', 66.6, 1024),
("2021-07-01 00:00:01.200", 'host1', 66.6, 1024),
("2021-07-01 00:00:02.200", 'host1', 66.6, 1024),
("2021-07-01 00:00:03.200", 'host1', 77.7, 2048),
("2021-07-01 00:00:04.200", 'host1', 77.7, 2048),
("2021-07-01 00:00:05.200", 'host1', 77.7, 2048),
("2021-07-01 00:00:06.200", 'host1', 88.8, 4096),
("2021-07-01 00:00:07.200", 'host1', 88.8, 4096),
("2021-07-01 00:00:08.200", 'host1', 88.8, 4096);

-- shouldn't truncate as window size is equal to total window length
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('avg_1m_flow');

ADMIN FLUSH_TABLE('avg_1m_monitor');

SELECT * FROM avg_1m_monitor ORDER BY time_window, host;

INSERT INTO monitor(ts, host, cpu, memory) VALUES
("2021-07-01 00:00:59.200", 'host1', 6.6, 224),
("2021-07-01 00:01:01.200", 'host1', 6.6, 224),
("2021-07-01 00:01:02.200", 'host1', 6.6, 224),
("2021-07-01 00:01:03.200", 'host1', 7.7, 248),
("2021-07-01 00:01:04.200", 'host1', 7.7, 248),
("2021-07-01 00:01:05.200", 'host1', 7.7, 248),
("2021-07-01 00:01:06.200", 'host1', 8.8, 296),
("2021-07-01 00:01:07.200", 'host1', 8.8, 296),
("2021-07-01 00:01:08.200", 'host1', 8.8, 296);

-- should truncate the old data before execute
-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('avg_1m_flow');

ADMIN FLUSH_TABLE('avg_1m_monitor');

SELECT * FROM avg_1m_monitor ORDER BY time_window, host;

DROP FLOW avg_1m_flow;
DROP TABLE avg_1m_monitor;
DROP TABLE monitor;
