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

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' FILL NULL ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL 6 FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL PREV FROM host ALIGN '5s'ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL LINEAR FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;
