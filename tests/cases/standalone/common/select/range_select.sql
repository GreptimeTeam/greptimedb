CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0),
    (5000,  'host1', null),
    (10000, 'host1', 1),
    (15000, 'host1', null),
    (20000, 'host1', 2),
    (0,     'host2', 3),
    (5000,  'host2', null),
    (10000, 'host2', 4),
    (15000, 'host2', null),
    (20000, 'host2', 5);

-- Test Fill

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FILL 6 FROM host ALIGN '5s' FILL NULL ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' FILL PREV ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' FILL LINEAR ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' FILL 6 ORDER BY host, ts;

-- Test nest sql

SELECT ts, host, foo FROM (SELECT ts, host, ((min(val)+max(val)) * 2) RANGE '10s' AS foo FROM host ALIGN '5s') WHERE foo > 8 ORDER BY host, ts;

SELECT ts, b, ((min(c)+max(c))/4) RANGE '10s' FROM (SELECT ts, host AS b, val AS c FROM host WHERE val > 2) ALIGN '5s' BY (b) ORDER BY b, ts;

-- Test range expr calculate

SELECT ts, host, (min(CAST(val AS Float64) * 2.0)/2) RANGE '10s', (max(CAST(val AS Float64) * 2.0)/2) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, covar(val, val) RANGE '20s', host FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, covar(ceil(CAST(val AS Float64)), floor(CAST(val AS Float64))) RANGE '20s', host FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, ceil((min(val * 2) + max(val * 2)) RANGE '20s' + 1.0) FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s' FILL PREV + min(val) RANGE '10s' FILL PREV FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, floor(cos(ceil(sin(min(val) RANGE '20s'))) )FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, gcd(max(val) RANGE '10s' FILL 1, max(val) RANGE '10s' FILL 1) * length(host) + 1 FROM host ALIGN '5s' ORDER BY host, ts;

-- Test Invalid cases

-- 1. error timestamp

SELECT min(val) RANGE 'not_time' FROM host ALIGN '5s';

SELECT min(val) RANGE '5s' FROM host ALIGN 'not_time';

-- 2.1 no range param

SELECT min(val) FROM host ALIGN '5s';

SELECT min(val) RANGE '10s', max(val) FROM host ALIGN '5s';

SELECT 1 RANGE '10s' FILL NULL FROM host ALIGN '1h' FILL NULL;

-- 2.2 no align param

SELECT min(val) RANGE '5s' FROM host;

-- 2.3 type mismatch

SELECT ts, covar(ceil(val), floor(val)) RANGE '20s', host FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' FILL 3.0 ORDER BY host, ts;

-- 2.4 nest query

SELECT ts, covar(max(val) RANGE '20s', floor(val)) RANGE '20s', host FROM host ALIGN '10s' ORDER BY host, ts;

-- 2.5 wrong Aggregate

SELECT max(ts) RANGE '20s', host FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, rank() OVER (PARTITION BY host ORDER BY ts DESC) RANGE '10s' FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;

-- Test on Timestamps of different precisions

CREATE TABLE host_sec (
  ts timestamp(0) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host_sec VALUES
    (0,  'host1', 0.0),
    (5,  'host1', 1.0),
    (10, 'host1', 2.0),
    (15, 'host1', 3.0),
    (20, 'host1', 4.0),
    (25, 'host1', 5.0),
    (30, 'host1', 6.0),
    (35, 'host1', 7.0),
    (40, 'host1', 8.0),
    (0,  'host2', 9.0),
    (5,  'host2', 10.0),
    (10, 'host2', 11.0),
    (15, 'host2', 12.0),
    (20, 'host2', 13.0),
    (25, 'host2', 14.0),
    (30, 'host2', 15.0),
    (35, 'host2', 16.0),
    (40, 'host2', 17.0);

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '10s' FROM host_sec ALIGN '5s' ORDER BY host, ts;

DROP TABLE host_sec;
