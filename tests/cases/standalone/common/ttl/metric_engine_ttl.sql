CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "", ttl = '1 second');

CREATE TABLE test_ttl (ts timestamp time index, val double, host string primary key) engine = metric with ("on_physical_table" = "phy");

INSERT INTO test_ttl(ts, val, host) VALUES
       (now(), 1, 'host1'),
       (now(), 2, 'host2'),
       (now(), 3, 'host3');

SELECT val, host FROM test_ttl;

-- SQLNESS SLEEP 2s
ADMIN flush_table('phy');

ADMIN compact_table('phy');

--- should be expired --
SELECT val, host FROM test_ttl;

ALTER TABLE phy SET ttl = '1 day';

INSERT INTO test_ttl(ts, val, host) VALUES
       (now(), 1, 'host1'),
       (now(), 2, 'host2'),
       (now(), 3, 'host3');

ADMIN flush_table('phy');

ADMIN compact_table('phy');

--- should not be expired --
SELECT val, host FROM test_ttl;

-- restart the db, ensure everything is ok
-- SQLNESS ARG restart=true
SELECT val, host FROM test_ttl;

DROP TABLE test_ttl;

DROP TABLE phy;
