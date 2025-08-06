TRUNCATE TABLE not_exists_table;

CREATE TABLE monitor (host STRING, ts TIMESTAMP, cpu DOUBLE DEFAULT 0, memory DOUBLE, TIME INDEX (ts), PRIMARY KEY(host));

INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1695217652000, 'host1', 66.6, 1024),
(1695217652000, 'host2', 66.6, 1024),
(1695217652000, 'host3', 66.6, 1024),
(1695217654000, 'host1', 77.7, 2048),
(1695217654000, 'host2', 77.7, 2048),
(1695217654000, 'host3', 77.7, 2048),
(1695217656000, 'host1', 88.8, 4096),
(1695217656000, 'host2', 88.8, 4096),
(1695217656000, 'host3', 88.8, 4096);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

TRUNCATE monitor;

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

-- truncate with time range
INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1695217652000, 'host1', 66.6, 1024),
(1695217652000, 'host2', 66.6, 1024),
(1695217652000, 'host3', 66.6, 1024),
(1695217654000, 'host1', 77.7, 2048),
(1695217654000, 'host2', 77.7, 2048),
(1695217654000, 'host3', 77.7, 2048),
(1695217656000, 'host1', 88.8, 4096),
(1695217656000, 'host2', 88.8, 4096),
(1695217656000, 'host3', 88.8, 4096);

ADMIN FLUSH_TABLE('monitor');

INSERT INTO monitor(ts, host, cpu, memory) VALUES
(1700000000111, 'host42', 66.6, 1024);

ADMIN FLUSH_TABLE('monitor');

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

TRUNCATE monitor FILE RANGE (0, 1700000000000), (1700000000111, 1700000000200);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

INSERT INTO monitor(ts, host, cpu, memory) VALUES 
(1695217660000, 'host1', 88.8, 4096),
(1695217662000, 'host2', 88.8, 4096),
(1695217664000, 'host3', 88.8, 4096);

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

TRUNCATE monitor;

SELECT ts, host, cpu, memory FROM monitor ORDER BY ts;

DROP TABLE monitor;


CREATE TABLE "MoNiToR" ("hOsT" STRING PRIMARY KEY, "tS" TIMESTAMP TIME INDEX, "cPu" DOUBLE DEFAULT 0);

TRUNCATE "MoNiToR";

DROP TABLE "MoNiToR";

CREATE TABLE MoNiToR (hOsT STRING PRIMARY KEY, tS TIMESTAMP TIME INDEX, cPu DOUBLE DEFAULT 0);

TRUNCATE MoNiToR;

DROP TABLE MoNiToR;
