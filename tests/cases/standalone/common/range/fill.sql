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

-- Test Fill when aggregate result is null

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' FILL NULL ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL 6 FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL PREV FROM host ALIGN '5s'ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL LINEAR FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;

-- Test Fill when time slot data is missing

CREATE TABLE host (
  ts timestamp(3) time index,
  host STRING PRIMARY KEY,
  val BIGINT,
);

INSERT INTO TABLE host VALUES
    (0,     'host1', 0),
    (1000,  'host1', 1),
    (2000, 'host1', 2),
    -- missing data for 5000, 10000
    (15000, 'host1', 6),
    (16000, 'host1', 7),
    (17000, 'host1', 8),

    (0,     'host2', 6),
    (1000,  'host2', 7),
    (2000,  'host2', 8),
    -- missing data for 5000, 10000
    (15000, 'host2', 12),
    (16000, 'host2', 13),
    (17000, 'host2', 14);

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FILL NULL FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FILL PREV FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FILL LINEAR FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FILL 6 FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s' FROM host ALIGN '5s' FILL NULL ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL NULL FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL 6 FROM host ALIGN '5s' ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL PREV FROM host ALIGN '5s'ORDER BY host, ts;

SELECT ts, host, min(val) RANGE '5s', min(val) RANGE '5s' FILL LINEAR FROM host ALIGN '5s' ORDER BY host, ts;

DROP TABLE host;
