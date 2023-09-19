CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val DOUBLE,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0.0),
    (5000,  'host1', 1.0),
    (10000, 'host1', 2.0),
    (15000, 'host1', 3.0),
    (20000, 'host1', 4.0),
    (25000, 'host1', 5.0),
    (30000, 'host1', 6.0),
    (35000, 'host1', 7.0),
    (40000, 'host1', 8.0),
    (0,     'host2', 9.0),
    (5000,  'host2', 10.0),
    (10000, 'host2', 11.0),
    (15000, 'host2', 12.0),
    (20000, 'host2', 13.0),
    (25000, 'host2', 14.0),
    (30000, 'host2', 15.0),
    (35000, 'host2', 16.0),
    (40000, 'host2', 17.0);

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '10s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val / 2.0)/2 RANGE '10s', max(val / 2.0)/2 RANGE '10s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, covar(val, val) RANGE '10s', host FROM host ALIGN '5s' ORDER BY host, ts;

SELECT covar(ceil(val), floor(val)) RANGE '10s', ts, host FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, covar((sin(val) + cos(val))/2.0 + 1.0, 2.0) RANGE '10s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, min(val) RANGE '10s', host, max(val) RANGE '10s' FROM host ALIGN '1000s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, (min(val)+max(val))/4 RANGE '10s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, foo FROM (SELECT ts, host, (min(val)+max(val))/4 RANGE '10s' AS foo FROM host ALIGN '5s' ORDER BY host, ts) WHERE foo > 5 ORDER BY host, ts;

SELECT ts, b, (min(c)+max(c))/4 RANGE '10s' FROM (SELECT ts, host AS b, val AS c FROM host WHERE val > 8.0) ALIGN '5s' BY (b) ORDER BY b, ts;

-- Test Invalid cases

-- 1. error timestamp

SELECT min(val) RANGE 'not_time' FROM host ALIGN '5s';

SELECT min(val) RANGE '5s' FROM host ALIGN 'not_time';

-- 2.1 no range param

SELECT min(val) FROM host ALIGN '5s';

SELECT min(val) RANGE '10s', max(val) FROM host ALIGN '5s';

-- 2.2 no align param

SELECT min(val) RANGE '5s' FROM host;

DROP TABLE host;

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

-- TODO(ruihang): This query returns incorrect result.
-- SELECT ts, host, min(val) RANGE '10s', max(val) RANGE '10s' FROM host_sec ALIGN '5s' ORDER BY host, ts;

DROP TABLE host_sec;
