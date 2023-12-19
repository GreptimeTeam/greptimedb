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

-- Test range expr calculate

SELECT ts, host, covar(val::DOUBLE, val::DOUBLE) RANGE '20s' FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, 2 * min(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val * 2) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val::DOUBLE) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(floor(val::DOUBLE)) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, floor(min(val) RANGE '5s') FROM host ALIGN '5s' ORDER BY host, ts;

-- Test complex range expr calculate

SELECT ts, host, (min(val) + max(val)) RANGE '20s' + 1.0 FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, covar(ceil(val::DOUBLE), floor(val::DOUBLE)) RANGE '20s' FROM host ALIGN '10s' ORDER BY host, ts;

SELECT ts, host, floor(cos(ceil(sin(min(val) RANGE '5s')))) FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, gcd(CAST(max(floor(val::DOUBLE)) RANGE '10s' FILL PREV as INT64) * 4, max(val * 4) RANGE '10s' FILL PREV) * length(host) + 1 FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;
